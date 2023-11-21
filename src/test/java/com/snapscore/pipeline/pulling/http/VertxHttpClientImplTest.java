package com.snapscore.pipeline.pulling.http;

import com.snapscore.pipeline.pulling.*;
import io.vertx.core.http.HttpClientOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.snapscore.pipeline.pulling.TestData.MATCH_DETAIL_FEED_NAME;

public class VertxHttpClientImplTest {


    private static final Logger log = LoggerFactory.getLogger(VertxHttpClientImplTest.class);
    private HttpClient httpClient;
    private PullingScheduler pullingScheduler;


    @Before
    public void setUp() throws Exception {

        Thread.sleep(5000);

        HttpClientConfig httpClientConfigMock = new HttpClientConfigMock();
        int receiveBufferSize = 4194304 * 10; // 40 MB

        final HttpClientOptions httpClientOptions = new HttpClientOptions()
                .setTryUseCompression(true)
                .setKeepAlive(true)
                .setConnectTimeout((int) httpClientConfigMock.readTimeout().toMillis())
                .setKeepAliveTimeout(20)
                .setMaxPoolSize(512)
                .setPoolCleanerPeriod(30000)
                .setIdleTimeout(30).setIdleTimeoutUnit(TimeUnit.SECONDS)
                .setTcpNoDelay(true)
                .setSendBufferSize(4096)
                .setReceiveBufferSize(receiveBufferSize)
                .setLogActivity(false)
                .setSsl(true)
                .setVerifyHost(false)
                .setTrustAll(true)
                .addEnabledSecureTransportProtocol("TLSv1.2");

        final WaitingRequestsTracker waitingRequestsTracker = new WaitingRequestsTrackerImpl(feedRequest -> feedRequest.getUrl());
        final ClientCallbackFactory vertxClientCallbackFactory = new VertxClientCallbackFactoryImpl(receiveBufferSize);
        httpClient = new VertxHttpClientImpl(httpClientConfigMock, httpClientOptions, vertxClientCallbackFactory);
        RequestsPerSecondCounter requestsPerSecondCounter = new RequestsPerSecondCounterImpl(Integer.MAX_VALUE);
        final PullingSchedulerQueue pullingSchedulerQueue =  new PullingSchedulerQueueImpl(httpClient, waitingRequestsTracker, requestsPerSecondCounter, FeedRequest.DEFAULT_PRIORITY_COMPARATOR);
        pullingScheduler = new PullingSchedulerImpl(pullingSchedulerQueue);

    }

    @After
    public void tearDown() throws Exception {
        Thread.sleep(10000);
        httpClient.shutdown();
    }

    @Ignore
    @Test
    public void pullingUrlOnlyOnce() {

        FeedRequest feedRequest = FeedRequest.newBuilder(MATCH_DETAIL_FEED_NAME, FeedPriorityEnum.MEDIUM,5, "https://www.google.com").build();

        Consumer<PullResult> pullResultConsumer = data -> {
            log.info("Received some data of byte length {} from feedRequest {}", data.getData().length, data.getFeedRequest());
            // Do stuff with the pulled data
        };

        pullingScheduler.pullOnce(feedRequest, pullResultConsumer, pullError -> {});
    }


    private static class HttpClientConfigMock implements HttpClientConfig {

        @Override
        public int getNumberOfThreads() {
            return 1;
        }

        @Override
        public Duration readTimeout() {
            return Duration.ofSeconds(30);
        }

        @Override
        public String host() {
            return "google.com";
        }

        @Override
        public int port() {
            return 443;
        }
    }

}