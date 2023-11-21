package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class PullingSchedulerImpl implements PullingScheduler {

    private static final Logger logger = Logger.setup(PullingSchedulerImpl.class);

    private final PullingSchedulerQueue pullingSchedulerQueue;

    public PullingSchedulerImpl(PullingSchedulerQueue pullingSchedulerQueue) {
        this.pullingSchedulerQueue = pullingSchedulerQueue;
    }


    @Override
    public <K> ScheduledDynamicRequests<K> schedulePullingDynamicRequests(K scheduledPullingKey,
                                                                          Supplier<List<FeedRequest>> feedRequestSupplier,
                                                                          Consumer<PullResult> pullResultConsumer,
                                                                          Consumer<PullError> pullErrorConsumer,
                                                                          Duration pullInterval,
                                                                          Duration initialDelay) {
        Disposable disposable = Flux.interval(initialDelay, pullInterval)
                .map(pullingCycleNo -> createFeedRequests(feedRequestSupplier))
                .map(feedRequests -> startPullingFeeds(scheduledPullingKey, feedRequests, pullResultConsumer, pullErrorConsumer))
                .subscribe(logger::info);
        return new ScheduledDynamicRequests<>(scheduledPullingKey, disposable, pullInterval);
    }

    private List<FeedRequest> createFeedRequests(Supplier<List<FeedRequest>> feedRequestSupplier) {
        try {
            return feedRequestSupplier.get();
        } catch (Exception e) {
            logger.error("Error creating feedRequests!", e);
            return Collections.emptyList();
        }
    }

    @Override
    public <K> ScheduledFixedRequests<K> schedulePullingFixedRequests(K scheduledPullingKey,
                                                                      List<FeedRequest> feedRequests,
                                                                      Consumer<PullResult> pullResultConsumer,
                                                                      Consumer<PullError> pullErrorConsumer,
                                                                      Duration pullInterval,
                                                                      Duration initialDelay) {
        Disposable disposable = Flux.interval(initialDelay, pullInterval)
                .map(pullingCycleNo -> startPullingFeeds(scheduledPullingKey, feedRequests, pullResultConsumer, pullErrorConsumer))
                .subscribe(logger::info);
        return new ScheduledFixedRequests<>(scheduledPullingKey, disposable, pullInterval, feedRequests);
    }

    private <K> String startPullingFeeds(K scheduledPullingKey,
                                         List<FeedRequest> feedRequests,
                                         Consumer<PullResult> pullResultConsumer,
                                         Consumer<PullError> pullErrorConsumer) {
        for (FeedRequest feedRequest : feedRequests) {
            pullingSchedulerQueue.enqueueForPulling(feedRequest, pullResultConsumer, pullErrorConsumer);
        }
        return "Scheduled pulling of " + feedRequests.size() + " feedRequests with scheduledPullingKey " + scheduledPullingKey;
    }

    @Override
    public <K> ScheduledFixedRequest<K> schedulePullingFixedRequest(K scheduledPullingKey,
                                                                    FeedRequestWithInterval feedRequest,
                                                                    Consumer<PullResult> pullResultConsumer,
                                                                    Consumer<PullError> pullErrorConsumer,
                                                                    Duration initialDelay) {
        Disposable disposable = Flux.interval(initialDelay, feedRequest.getPullInterval())
                .map(pullingCycleNo -> {
                    pullingSchedulerQueue.enqueueForPulling(feedRequest, pullResultConsumer, pullErrorConsumer);
                    return "Scheduled pulling of " + feedRequest;
                })
                .subscribe(logger::info);
        return new ScheduledFixedRequest<>(scheduledPullingKey, feedRequest, disposable);
    }

    @Override
    public <K> ScheduledFixedBoundedRequest<K> pullNumberOfTimes(K scheduledPullingKey,
                                                                 FeedRequestWithInterval feedRequest,
                                                                 Consumer<PullResult> pullResultConsumer,
                                                                 Consumer<PullError> pullErrorConsumer,
                                                                 int repeatTimes,
                                                                 Duration initialDelay) {
        List<Mono<String>> monoList = new ArrayList<>();

        Function<Duration, Mono<String>> monoCreator = duration -> Mono.just(1)
                .delayElement(feedRequest.getPullInterval())
                .map(dummy -> {
                    pullingSchedulerQueue.enqueueForPulling(feedRequest, pullResultConsumer, pullErrorConsumer);
                    return "Scheduled pulling of " + feedRequest;
                });

        Mono<String> initiallyDelayedMono = monoCreator.apply(initialDelay);
        monoList.add(initiallyDelayedMono);

        for (int idx = 0; idx < repeatTimes; idx++) {
            Mono<String> mono = monoCreator.apply(feedRequest.getPullInterval());
            monoList.add(mono);
        }

        Disposable disposable = Flux.concat(monoList).subscribe(logger::info);
        return new ScheduledFixedBoundedRequest<>(scheduledPullingKey, feedRequest, disposable, repeatTimes);
    }

    // must be synchronized -> is called from multiple threads and accesses data that is not thread-safe and operations on it need to be atomic
    @Override
    public synchronized void pullOnce(FeedRequest feedRequest,
                                      Consumer<PullResult> pullResultConsumer,
                                      Consumer<PullError> pullErrorConsumer) {
        pullingSchedulerQueue.enqueueForPulling(feedRequest, pullResultConsumer, pullErrorConsumer);
    }

}
