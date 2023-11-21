package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;
import com.snapscore.pipeline.pulling.http.HttpClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PullingSchedulerQueueImpl implements PullingSchedulerQueue {

    private static final Logger logger = Logger.setup(PullingSchedulerQueueImpl.class);

    private final HttpClient httpClient;
    private final Queue<QueueFeedRequest> requestsQueue;
    private final WaitingRequestsTracker waitingRequestsTracker;
    private final RequestsPerSecondCounter requestsPerSecondCounter;

    public static final Duration PERIODIC_PULL_NEXT_TRIGGER_INTERVAL = Duration.ofMillis(5);
    private final Duration periodicPullNextTriggerInterval;
    private volatile Supplier<LocalDateTime> nowSupplier;
    private final boolean ignoreDelayedRequests;

    public PullingSchedulerQueueImpl(HttpClient httpClient,
                                     WaitingRequestsTracker waitingRequestsTracker,
                                     RequestsPerSecondCounter requestsPerSecondCounter,
                                     Comparator<FeedRequest> requestsPrioritizingComparator) {
        this(httpClient, waitingRequestsTracker, requestsPerSecondCounter, requestsPrioritizingComparator, false);
    }

    public PullingSchedulerQueueImpl(HttpClient httpClient,
                                     WaitingRequestsTracker waitingRequestsTracker,
                                     RequestsPerSecondCounter requestsPerSecondCounter,
                                     Comparator<FeedRequest> requestsPrioritizingComparator,
                                     boolean ignoreDelayedRequests) {
        this(
                httpClient,
                waitingRequestsTracker,
                requestsPerSecondCounter,
                requestsPrioritizingComparator,
                PERIODIC_PULL_NEXT_TRIGGER_INTERVAL, // sensible default
                LocalDateTime::now,
                ignoreDelayedRequests);
    }

    /**
     * FOR TESTING PURPOSES ONLY
     * package private access is intentional
     *
     * @param periodicPullNextTriggerInterval helps testability
     * @param nowSupplier                     helps testability
     * @param ignoreDelayedRequests
     */
    PullingSchedulerQueueImpl(HttpClient httpClient,
                              WaitingRequestsTracker waitingRequestsTracker,
                              RequestsPerSecondCounter requestsPerSecondCounter,
                              Comparator<FeedRequest> requestsPrioritizingComparator,
                              Duration periodicPullNextTriggerInterval,
                              Supplier<LocalDateTime> nowSupplier,
                              boolean ignoreDelayedRequests) {
        this.httpClient = httpClient;
        this.waitingRequestsTracker = waitingRequestsTracker;
        this.requestsPerSecondCounter = requestsPerSecondCounter;
        this.requestsQueue = new PriorityBlockingQueue<>(100, QueueFeedRequest.makeComparatorFrom(requestsPrioritizingComparator));
        this.periodicPullNextTriggerInterval = periodicPullNextTriggerInterval;
        this.nowSupplier = nowSupplier;
        this.ignoreDelayedRequests = ignoreDelayedRequests;
        schedulePeriodicPullNextTrigger();
    }

    PullingSchedulerQueueImpl(HttpClient httpClient,
                              WaitingRequestsTracker waitingRequestsTracker,
                              RequestsPerSecondCounter requestsPerSecondCounter,
                              Comparator<FeedRequest> requestsPrioritizingComparator,
                              Duration periodicPullNextTriggerInterval,
                              Supplier<LocalDateTime> nowSupplier) {
        this(httpClient, waitingRequestsTracker, requestsPerSecondCounter, requestsPrioritizingComparator, periodicPullNextTriggerInterval, nowSupplier, false);
    }

    private void schedulePeriodicPullNextTrigger() {
        Flux.interval(periodicPullNextTriggerInterval, periodicPullNextTriggerInterval)
                .doOnNext(num -> {
//                    jsonLog.debug("Running regular trigger of pullNext()");
                    this.dequeueNextAndPull();
                })
                .onErrorResume(throwable -> {
                    logger.warn("Error in scheduled trigger of pullNext()", throwable);
                    return Mono.empty();
                })
                .subscribe();
    }


    @Override
    public void enqueueForPulling(FeedRequest feedRequest,
                                               Consumer<PullResult> pullResultConsumer,
                                               Consumer<PullError> pullErrorConsumer) {
        enqueueRequest(feedRequest, pullResultConsumer, pullErrorConsumer);
        dequeueNextAndPull();
    }

    /**
     * must be synchronized -> is called from multiple threads and accesses data that is not thread-safe and operations on it need to be atomic
     */
    private synchronized void enqueueRequest(FeedRequest feedRequest, Consumer<PullResult> pullResultConsumer, Consumer<PullError> pullErrorConsumer) {
        if (shouldMakeRequest(feedRequest)) {
            requestsQueue.add(new QueueFeedRequest(feedRequest, pullResultConsumer, pullErrorConsumer, System.currentTimeMillis()));
            waitingRequestsTracker.trackAwaitingResponse(feedRequest);
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).info("New enqueued request info: {}", feedRequest.toStringBasicInfo());
            logEnqueuedRequestCount();
        } else {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).analyticsId("ignoring_duplicate_request")).info("FeedRequest already enqueued for pulling - ignoring: {}; ", feedRequest.toStringBasicInfo());
        }
    }

    boolean shouldMakeRequest(FeedRequest feedRequest) {
        return (!waitingRequestsTracker.isAwaitingResponse(feedRequest) && !(ignoreDelayedRequests && waitingRequestsTracker.isAwaitingRetry(feedRequest))) || isAwaitingForTooLong(feedRequest);
    }

    private boolean isAwaitingForTooLong(FeedRequest feedRequest) {
        Optional<TrackedRequest> trackedRequest = waitingRequestsTracker.getTrackedRequest(feedRequest);
        if (trackedRequest.isPresent()) {
            Duration trackedDuration = trackedRequest.get().trackedDuration();
            if (trackedDuration.getSeconds() > 60 * 60_000) { // 60 mins
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).analyticsId("request_tracked_too_long")).warn(">>> !!! Request is tracked for too long! It is possible that some pulling results are not getting emitted by the http client callback implementation and it is causing requests being incorrectly ignored. It can also mean that too many low priority requests are stuck in the queue for too long because they do not get a chance to be processed due to higher priority requests. <<< {}", feedRequest);
                return true;
            }
        }
        return false;
    }


    /**
     * must be synchronized -> is called from multiple threads and accesses data that is not thread-safe and operations on it need to be atomic
     */
    private synchronized void dequeueNextAndPull() {
        try {
            QueueFeedRequest nextQueueRequest = requestsQueue.peek();
            while (nextQueueRequest != null && requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(nowSupplier.get())) {
                requestsQueue.poll(); // remove from queue head
                scheduleSinglePull(nextQueueRequest.getFeedRequest(),
                        nextQueueRequest.getPullResultConsumer(),
                        nextQueueRequest.getPullErrorConsumer(),
                        nextQueueRequest.getEnqueuedTimestamp()
                );
                nextQueueRequest = requestsQueue.peek();
            }
        } catch (Exception e) {
            logger.error("Error pulling next request!", e);
        }
    }

    private void scheduleSinglePull(FeedRequest request,
                                    Consumer<PullResult> pullResultConsumer,
                                    Consumer<PullError> pullErrorConsumer,
                                    long enqueuedTimestamp) {

        AtomicBoolean isRetry = new AtomicBoolean(false);

        Mono.just(request)
                .map(request0 -> handleRequestIfRetried(isRetry, request0))
                .flatMap(canProceed -> Mono.fromFuture(httpClient.getAsync(request))) // if we got her eit means that the previous step passed and emmited 'true'
                .publishOn(Schedulers.parallel())   // emitted results need to be published on parallel scheduler so we do not execute pulled data processing on the httpClient's own threadpool
                .onErrorMap(error -> {
                    logRequestError(request, error);
                    waitingRequestsTracker.untrackProcessed(request);   // if an error happened and will be retried at some point, we want to untrack the request so that other requests coming in for the same url do not get ignored
                    waitingRequestsTracker.trackAwaitingRetry(request);
                    return error;
                })
                .retryWhen(Retry.backoff(request.getNumOfRetries(), request.getRetryBackoff()))
                .onErrorResume(error -> {
                    logDroppingRetrying(request, error);
                    logEnqueuedRequestCount();
                    notifyOnErrorCallback(request, pullErrorConsumer, error);
                    return Mono.empty();
                    // note - do not untrack request here, it should already be untracked by onErrorMap() above
                })
                .doOnNext(data -> {
                    waitingRequestsTracker.untrackProcessed(request);
                    logEnqueuedRequestCount();
                    logRequestProcessed(request, enqueuedTimestamp);
                })
                .map(rawData -> new PullResult(request, rawData))
                .subscribe(pullResultConsumer);
    }


    // returns true if te request is within limit. The returned value has no affect though on subsequent items in the Mono chain
    private Mono<Boolean> handleRequestIfRetried(AtomicBoolean isRetry, FeedRequest request) {
        // done like this with AtomicBoolean because when we poll requests from requestsQueue
        // we have checked that they are within limit so that is ok ...
        // ... but when requests fail and are retried by the Reactor Flux we need to check again
        // the retry because it happens at some later point and we might have run out of rqs / sec for that moment
        // ->>> WE NEED TO MAKE SURE THAT RETRIED REQUEST ALSO RESPECT THE RQs/SEC LIMIT + THAT THEY ARE TRACKED CORRECTLY
        if (isRetry.get()) {
            if (requestsPerSecondCounter.incrementIfRequestWithinLimitAndGet(nowSupplier.get())) {
                logRetry(request);
                waitingRequestsTracker.untrackRetried(request);
                waitingRequestsTracker.trackAwaitingResponse(request);
                return Mono.just(true);
            } else {
                // repeat until we are within limit ...
                logDelayedRetry(request);
                return Mono.just(false)
                        .delayElement(periodicPullNextTriggerInterval)
                        .flatMap(dummy -> handleRequestIfRetried(isRetry, request)); // call this method again ... kind of recursively ... until we are within limit at some point

            }
        } else {
            isRetry.set(true); // any subsequent traversal of this mono can only be a retry
            return Mono.just(true);
        }
    }

    private void notifyOnErrorCallback(FeedRequest request, Consumer<PullError> pullErrorConsumer, Throwable error) {
        try {
            pullErrorConsumer.accept(new PullError(request, error));
        } catch (Exception e) {
            logger.decorateSetup(mdc -> mdc.anyId(request.getUuid())).error("Error in pullErrorConsumer callback: ", e);
        }
    }

    private void logRequestError(FeedRequest request, Throwable error) {
        logger.decorateSetup(mdc -> mdc.anyId(request.getUuid()).analyticsId("request_error")).warn("Error for request: {}", request.toStringBasicInfo(), error);
    }

    private void logRetry(FeedRequest request) {
        logger.decorateSetup(mdc -> mdc.anyId(request.getUuid()).analyticsId("request_retry")).info("Going to retry request after previous failure {}", request.toStringBasicInfo());
    }

    private void logDelayedRetry(FeedRequest request) {
        logger.decorateSetup(mdc -> mdc.anyId(request.getUuid()).analyticsId("request_retry_delay")).info("Cannot retry request yet - due to rqs per sec. limit {}", request.toStringBasicInfo());
    }

    private void logEnqueuedRequestCount() {
        logger.info("Currently enqueued rqs count = {}", waitingRequestsTracker.countOfRequestsAwaitingResponse());
    }

    private void logRequestProcessed(FeedRequest request, long enqueuedTimestamp) {
        final double processingTime = (System.currentTimeMillis() - enqueuedTimestamp) / 1000.0;
        logger.info("Request took {}s to process: {}", String.format("%.2f", processingTime), request);
    }

    private void logDroppingRetrying(FeedRequest request, Throwable error) {
        logger.decorateSetup(mdc -> mdc.anyId(request.getUuid()).analyticsId("dropped_failed_request")).warn("Dropping request retry {} after error: ", request.toStringBasicInfo(), error);
    }


    /**
     * FOR TESTING PURPOSES ONLY.
     * Needed so we are able to test changes of behaviour based on the passage of time
     * Package-private visibility intentional
     */
    void setNowSupplier(Supplier<LocalDateTime> nowSupplierNew) {
        this.nowSupplier = nowSupplierNew;
    }
}
