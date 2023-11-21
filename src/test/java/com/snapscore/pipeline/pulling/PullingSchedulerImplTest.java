package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.pulling.http.*;
import io.vertx.core.http.HttpClientOptions;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.snapscore.pipeline.pulling.TestData.MATCH_DETAIL_FEED_NAME;
import static com.snapscore.pipeline.pulling.TestData.STAGE_FIXTURES_FEED_NAME;
import static org.junit.Assert.*;

public class PullingSchedulerImplTest {

    private static final Logger log = LoggerFactory.getLogger(PullingSchedulerImplTest.class);
    private static byte[] pulledData = "data".getBytes();

    private HttpClient httpClientMock;
    private PullingScheduler pullingScheduler;
    private WaitingRequestsTracker waitingRequestsTracker;

    @Before
    public void setUp() throws Exception {
        httpClientMock = new HttpClientMock();
        waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        pullingScheduler = newPullingScheduler(requestsPerSecondCounter, false);
    }

    private PullingScheduler newPullingScheduler(RequestsPerSecondCounter requestsPerSecondCounter, boolean ignoreDelayedRequests) {
        PullingSchedulerQueue pullingSchedulerQueue = new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofDays(1), () -> LocalDateTime.now(), ignoreDelayedRequests);
        return new PullingSchedulerImpl(pullingSchedulerQueue);
    }

    @Test
    public void pullOnce() throws InterruptedException {

        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_1").build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {
        });

        Thread.sleep(100);

        Mockito.verify(pullResultConsumerMock, Mockito.times(1)).accept(new PullResult(feedRequest, pulledData));
    }


    @Test
    public void schedulePullingDynamicRequests() throws InterruptedException {

        // given
        List<FeedRequest> feedRequests = List.of(
                FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_1").build(),
                FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_2").build()
        );
        Supplier<List<FeedRequest>> feedRequestsSupplier = () -> feedRequests;

        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);
        TestData.FeedNameEnum scheduledPullingKey = TestData.FeedNameEnum.MATCH_DETAIL_FEED;

        // when
        pullingScheduler.schedulePullingDynamicRequests(scheduledPullingKey, feedRequestsSupplier, pullResultConsumerMock, pullError -> {
        }, Duration.ofMillis(10), Duration.ZERO);
        Thread.sleep(100);

        // then
        ArgumentCaptor<PullResult> pullResultArgumentCaptor = ArgumentCaptor.forClass(PullResult.class);
        Mockito.verify(pullResultConsumerMock, Mockito.atLeast(10)).accept(pullResultArgumentCaptor.capture());
        assertRequestPulledAtLeast(5, feedRequests.get(0), pullResultArgumentCaptor);
        assertRequestPulledAtLeast(5, feedRequests.get(1), pullResultArgumentCaptor);
    }

    @Test
    @Ignore("This test fails randomly, ignoring it")
    public void cancellingScheduledDynamicRequests() throws InterruptedException {

        // given
        Supplier<List<FeedRequest>> feedRequestsSupplier = () -> {
            return List.of(FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_1_&timestamp=" + System.currentTimeMillis()).build(),
                    FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_2_&timestamp=" + System.currentTimeMillis()).build()
            );
        };

        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);
        TestData.FeedNameEnum scheduledPullingKey = TestData.FeedNameEnum.MATCH_DETAIL_FEED;
        ScheduledDynamicRequests<TestData.FeedNameEnum> scheduledDynamicRequests = pullingScheduler.schedulePullingDynamicRequests(scheduledPullingKey, feedRequestsSupplier, pullResultConsumerMock, pullError -> {
        }, Duration.ofMillis(1), Duration.ZERO);
        Thread.sleep(100); // give async code time to run
        Mockito.verify(pullResultConsumerMock, Mockito.atLeast(10)).accept(Mockito.any());

        // when
        scheduledDynamicRequests.cancel();
        Mockito.reset(pullResultConsumerMock);
        Thread.sleep(100); // give async code time to run

        // then
        Mockito.verify(pullResultConsumerMock, Mockito.atMost(4)).accept(Mockito.any()); // at most four times as when as when we cancel there might just be one request being processed that will return the data
    }


    @Test
    public void schedulePullingFixedRequest() throws InterruptedException {

        // given
        FeedRequestWithInterval feedRequest = FeedRequestWithInterval.newBuilder(STAGE_FIXTURES_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_1", Duration.ofMillis(10)).build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);
        String scheduledPullingKey = TestData.FeedNameEnum.STAGE_FIXTURES_FEED + "_stageId_123";

        // when
        pullingScheduler.schedulePullingFixedRequest(scheduledPullingKey, feedRequest, pullResultConsumerMock, pullError -> {
        }, Duration.ZERO);

        Thread.sleep(100); // give async code time to run

        // then
        Mockito.verify(pullResultConsumerMock, Mockito.atLeast(5)).accept(new PullResult(feedRequest, pulledData));
    }


    @Test
    public void cancellingScheduledFixedRequest() throws InterruptedException {

        // given
        FeedRequestWithInterval feedRequest = FeedRequestWithInterval.newBuilder(STAGE_FIXTURES_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_1", Duration.ofMillis(1)).build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);
        String scheduledPullingKey = TestData.FeedNameEnum.STAGE_FIXTURES_FEED + "_stageId_123";
        ScheduledFixedRequest<String> scheduledFixedRequest = pullingScheduler.schedulePullingFixedRequest(scheduledPullingKey, feedRequest, pullResultConsumerMock, pullError -> {
        }, Duration.ZERO);
        Thread.sleep(100); // give async code time to run
        Mockito.verify(pullResultConsumerMock, Mockito.atLeast(5)).accept(new PullResult(feedRequest, pulledData));

        // when
        scheduledFixedRequest.cancel();
        Mockito.reset(pullResultConsumerMock);
        Thread.sleep(100); // give async code time to run

        // then
        Mockito.verify(pullResultConsumerMock, Mockito.atMost(2)).accept(Mockito.any()); // at most once as when as when we cancel there might just be one request being processed that will return the data
    }


    @Test
    public void schedulePullingFixedRequests() throws InterruptedException {

        // given
        FeedRequestWithInterval feedRequest = FeedRequestWithInterval.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_1", Duration.ofMillis(10)).build();

        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);
        String scheduledPullingKey = "stage_id_123_match_details";

        // when
        ScheduledFixedRequest<String> scheduledFixedRequest = pullingScheduler.schedulePullingFixedRequest(scheduledPullingKey, feedRequest, pullResultConsumerMock, pullError -> {
        }, Duration.ZERO);
        Thread.sleep(200); // give async code time to run

        // then
        ArgumentCaptor<PullResult> pullResultArgumentCaptor = ArgumentCaptor.forClass(PullResult.class);
        Mockito.verify(pullResultConsumerMock, Mockito.atLeast(10)).accept(pullResultArgumentCaptor.capture());
    }

    private void assertRequestPulledAtLeast(int minNumberOfPulls, FeedRequest feedRequest, ArgumentCaptor<PullResult> pullResultArgumentCaptor) {
        assertTrue(minNumberOfPulls <= pullResultArgumentCaptor.getAllValues().stream().filter(pullResult -> pullResult.getFeedRequest().getUrl().equals(feedRequest.getUrl())).count());
    }

    private void assertRequestPulledAtMost(int maxNumberOfPulls, FeedRequest feedRequest, ArgumentCaptor<PullResult> pullResultArgumentCaptor) {
        assertTrue(maxNumberOfPulls >= pullResultArgumentCaptor.getAllValues().stream().filter(pullResult -> pullResult.getFeedRequest().getUrl().equals(feedRequest.getUrl())).count());
    }


    @Test
    public void testThatFailedRequestGetsRetried() throws InterruptedException {

        HttpClientFailingMock httpClientMock = new HttpClientFailingMock();
        waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofDays(1), () -> LocalDateTime.now(), false);
        pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        final Duration retryDelay = Duration.ZERO;
        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 9, "url_1").setRetryDelaySupplier(rq -> retryDelay).build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {
        });

        Thread.sleep(100);

        assertEquals(10, httpClientMock.invocationCounter.get());
        Mockito.verify(pullResultConsumerMock, Mockito.times(0)).accept(Mockito.any());
    }

    @Test
    public void testThatIfFailedRequestGetsRetriedAndDroppedThenScheduledPullingContinues() throws InterruptedException {

        HttpClientFailingMock httpClientMock = new HttpClientFailingMock();
        waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofDays(1), () -> LocalDateTime.now(), false);
        pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        final Duration retryDelay = Duration.ZERO;
        FeedRequestWithInterval feedRequest = FeedRequestWithInterval.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 2, "url_1", Duration.ofMillis(500)).setRetryDelaySupplier(rq -> retryDelay).build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        pullingScheduler.schedulePullingFixedRequest(MATCH_DETAIL_FEED_NAME, feedRequest, pullResultConsumerMock, pullError -> {
        }, Duration.ZERO);

        Thread.sleep(1400);

        assertEquals(9, httpClientMock.invocationCounter.get());
        Mockito.verify(pullResultConsumerMock, Mockito.times(0)).accept(Mockito.any());
    }

    @Test
    public void testThatRequestPerSecondLimitIsRespected() throws InterruptedException {

        // given
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        LocalDateTime now = LocalDateTime.of(2020, 1, 1, 12, 0, 1);
        Supplier<LocalDateTime> nowSupplier1 = () -> now;
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(10, now.minus(1000L, ChronoUnit.MILLIS));
        final PullingSchedulerQueueImpl pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofMillis(1000), nowSupplier1, false);
        PullingSchedulerImpl pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        // when
        for (int requestNo = 1; requestNo <= 20; requestNo++) {
            FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_" + requestNo).build();
            pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {
            });
        }

        // when first requests per second limit is reached only first half of requests is sent ...
        Thread.sleep(200);

        Mockito.verify(pullResultConsumerMock, Mockito.times(10)).accept(Mockito.any());
        Mockito.reset(pullResultConsumerMock);

        // ... then after 1 second passes ... the next half of requests gets sent
        Supplier<LocalDateTime> nowSupplier2 = () -> now.plusSeconds(1);
        pullingSchedulerQueue.setNowSupplier(nowSupplier2); // simulates passage of time

        Thread.sleep(2000);

        Mockito.verify(pullResultConsumerMock, Mockito.times(10)).accept(Mockito.any());

    }


    @Test
    public void testThatRequestPerSecondLimitIsRespectedWhileThereAreRetriedFailedRequests() throws InterruptedException {

        // given
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        LocalDateTime now = LocalDateTime.of(2020, 1, 1, 12, 0, 1);
        Supplier<LocalDateTime> nowSupplier1 = () -> now;
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(10, now.minus(1000L, ChronoUnit.MILLIS));
        HttpClientFailingMock httpClientMock = new HttpClientFailingMock();
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofMillis(1000), nowSupplier1, false);
        PullingSchedulerImpl pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        // when ... a requests gets retried ... it consumes the requests quota ...
        final Duration retryDelay = Duration.ZERO;
        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 4, "url_" + 0).setRetryDelaySupplier(rq -> retryDelay).build();
        pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {
        });
        Thread.sleep(200);

        assertEquals(5, httpClientMock.invocationCounter.get());

        // ... and when other requests are scheduled ... they can run out of qouta per second ....
        for (int requestNo = 0; requestNo < 20; requestNo++) {
            FeedRequest subsequentRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 0, "url_" + requestNo).build();
            pullingScheduler.pullOnce(subsequentRequest, pullResultConsumerMock, pullError -> {
            });
        }

        // ... then only part of them was handled if we are still in the same second of time
        Thread.sleep(200);
        assertEquals(5 + 5, httpClientMock.invocationCounter.get());

    }

    @Test
    public void testThatDuplicateRequestWillGetIgnored() throws InterruptedException {

        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 1, "url_1").build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        for (int i = 0; i < 2; i++)
            pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {});

        Thread.sleep(100);

        Mockito.verify(pullResultConsumerMock, Mockito.times(1)).accept(new PullResult(feedRequest, pulledData));
    }

    @Test
    public void testThatFailedRequestGetsStored() throws InterruptedException {
        HttpClientFailingMock httpClientMock = new HttpClientFailingMock();
        waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofDays(1), () -> LocalDateTime.now(), false);
        pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        final Duration retryDelay = Duration.ZERO;
        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 9, "url_1").setRetryDelaySupplier(rq -> retryDelay).build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {});

        Thread.sleep(100);

        assertFalse(waitingRequestsTracker.isAwaitingResponse(feedRequest));
        assertTrue(waitingRequestsTracker.isAwaitingRetry(feedRequest));
    }

    @Test
    public void testThatDuplicateRetriedRequestGetsIgnored() throws InterruptedException {
        HttpClientFailingMock httpClientMock = new HttpClientFailingMock();
        waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofDays(1), () -> LocalDateTime.now(), true);
        pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        final Duration retryDelay = Duration.ZERO;
        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 9, "url_1").setRetryDelaySupplier(rq -> retryDelay).build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {});

        Thread.sleep(100);

        assertFalse(((PullingSchedulerQueueImpl) pullingSchedulerQueue).shouldMakeRequest(feedRequest));
    }

    @Test
    public void testThatFailedRequestGetsRemovedAfterSuccessfulRetry() throws InterruptedException {
        HttpClientFirstFailMock httpClientMock = new HttpClientFirstFailMock();
        waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClientMock, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR, Duration.ofDays(1), () -> LocalDateTime.now(), false);
        pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        final Duration retryDelay = Duration.ZERO;
        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 9, "url_1").setRetryDelaySupplier(rq -> retryDelay).build();
        Consumer<PullResult> pullResultConsumerMock = Mockito.mock(Consumer.class);

        for (int i = 0; i < 2; i++)
            pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullError -> {});

        Thread.sleep(100);

        assertFalse(waitingRequestsTracker.isAwaitingResponse(feedRequest));
        assertFalse(waitingRequestsTracker.isAwaitingRetry(feedRequest));
    }


    @Ignore // keep ignored
    @Test
    public void testPullingRealEnetEP() throws InterruptedException {

        HttpClientConfig httpClientConfig = new HttpClientConfig() {
            @Override
            public int getNumberOfThreads() {
                return 16;
            }

            @Override
            public Duration readTimeout() {
                return Duration.ofSeconds(15);
            }

            @Override
            public String host() {
                return "eapi.enetpulse.com";
            }

            @Override
            public int port() {
                return 80;
            }
        };

        WaitingRequestsTracker waitingRequestsTrackerWithNoTracking = new WaitingRequestsTracker() {
            @Override
            public boolean isAwaitingResponse(FeedRequest feedRequest) {
                return false;
            }

            @Override
            public void trackAwaitingResponse(FeedRequest feedRequest) {
                // do nothing
            }

            @Override
            public Optional<TrackedRequest> getTrackedRequest(FeedRequest feedRequest) {
                return Optional.empty();
            }

            @Override
            public void untrackProcessed(FeedRequest feedRequest) {
                // do nothing
            }

            @Override
            public int countOfRequestsAwaitingResponse() {
                return 0;
            }

            @Override
            public boolean isAwaitingRetry(FeedRequest feedRequest) {
                return false;
            }

            @Override
            public void trackAwaitingRetry(FeedRequest feedRequest) {

            }

            @Override
            public void untrackRetried(FeedRequest feedRequest) {

            }
        };

        int httpResponseBufferSize = 4194304 * 10;
        HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setTryUseCompression(true)
                .setKeepAlive(true)
                .setConnectTimeout(15000)
                .setKeepAliveTimeout(20)
                .setMaxPoolSize(512)
                .setPoolCleanerPeriod(30000)
                .setIdleTimeout(30).setIdleTimeoutUnit(TimeUnit.SECONDS)
                .setTcpNoDelay(true)
                .setSendBufferSize(4096)
                .setReceiveBufferSize(httpResponseBufferSize) // 40 MB
                .setLogActivity(false)
                .setSsl(false)
                .setVerifyHost(false)
                .setTrustAll(true)
                .addEnabledSecureTransportProtocol("TLSv1.2");


        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM, 5, "http://eapi.enetpulse.com/event/details/?id=3466032&includeLineups=no&includeEventProperties=yes&includeIncidents=yes&includeExtendedResults=yes&includeProperties=yes&includeLivestats=yes&includeVenue=yes&username=snaptechapiusr&token=07af955d48282ad7f50689d882066956").build();
        Consumer<PullResult> pullResultConsumerMock = pullResult -> System.out.println("Received data");
        Consumer<PullError> pullErrorConsumer = pullError -> System.out.println("Failed to get data!");

        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(5);
        ClientCallbackFactory<VertxClientCallback> clientCallbackFactory = new VertxClientCallbackFactoryImpl(httpResponseBufferSize);
        HttpClient httpClient = new VertxHttpClientImpl(httpClientConfig, httpClientOptions, clientCallbackFactory);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClient, waitingRequestsTrackerWithNoTracking, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR);
        PullingSchedulerImpl pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

        for (int i = 0; i < 20; i++) {
            pullingScheduler.pullOnce(feedRequest, pullResultConsumerMock, pullErrorConsumer);
        }

        Thread.sleep(100_000);

    }


    private static class HttpClientMock implements HttpClient {

        @Override
        public CompletableFuture<byte[]> getAsync(FeedRequest feedRequest) {
            log.info("Pulling {}", feedRequest.toStringBasicInfo());
            CompletableFuture<byte[]> completableFuture = new CompletableFuture<>();
            completableFuture.complete(pulledData);
            return completableFuture;
        }

        @Override
        public void shutdown() {
        }

    }

    private static class HttpClientFailingMock implements HttpClient {

        private AtomicInteger invocationCounter = new AtomicInteger(0);

        @Override
        public CompletableFuture<byte[]> getAsync(FeedRequest feedRequest) {
            invocationCounter.incrementAndGet();
            log.info("Pulling {}", feedRequest.toStringBasicInfo());
            throw new RuntimeException("Simulated error ...", new FailedRequestException(feedRequest));
        }

        @Override
        public void shutdown() {
        }

    }

    private static class HttpClientFirstFailMock implements HttpClient {

        private final AtomicBoolean failed = new AtomicBoolean(false);

        @Override
        public CompletableFuture<byte[]> getAsync(FeedRequest feedRequest) {
            log.info("Pulling {}", feedRequest.toStringBasicInfo());
            if (!failed.getAndSet(true)) throw new RuntimeException("Simulated error ...", new FailedRequestException(feedRequest));

            CompletableFuture<byte[]> completableFuture = new CompletableFuture<>();
            completableFuture.complete(pulledData);
            return completableFuture;
        }

        @Override
        public void shutdown() {
        }

    }


}
