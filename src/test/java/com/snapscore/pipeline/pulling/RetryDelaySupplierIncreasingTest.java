package com.snapscore.pipeline.pulling;

import org.junit.Test;

import java.time.Duration;

import static com.snapscore.pipeline.pulling.RetryDelaySupplier.DEFAULT_FIRST_RETRY_BACKOFF;
import static org.junit.Assert.assertEquals;

public class RetryDelaySupplierIncreasingTest {

    @Test
    public void calcBackoff() {
        RetryDelaySupplier retryDelaySupplier = new RetryDelaySupplierIncreasing();
        FeedRequestWithInterval feedRequest = new FeedRequestWithInterval(null, null, null, Duration.ofSeconds(60), 0, null, null, null);
        assertEquals(Duration.ofSeconds(30), retryDelaySupplier.calcBackoff(feedRequest));
        feedRequest = new FeedRequestWithInterval(null, null, null, Duration.ofSeconds(60), 1, null, null, null);
        assertEquals(Duration.ofSeconds(60), retryDelaySupplier.calcBackoff(feedRequest));
        feedRequest = new FeedRequestWithInterval(null, null, null, Duration.ofSeconds(60), 2, null, null, null);
        assertEquals(Duration.ofSeconds(120), retryDelaySupplier.calcBackoff(feedRequest));
        feedRequest = new FeedRequestWithInterval(null, null, null, Duration.ofSeconds(60), 3, null, null, null);
        assertEquals(Duration.ofSeconds(240), retryDelaySupplier.calcBackoff(feedRequest));
        feedRequest = new FeedRequestWithInterval(null, null, null, Duration.ofSeconds(60), 4, null, null, null);
        assertEquals(Duration.ofSeconds(480), retryDelaySupplier.calcBackoff(feedRequest));

        FeedRequest feedRequest2 = new FeedRequest(null, null, null, 0, null, null, null);
        assertEquals(DEFAULT_FIRST_RETRY_BACKOFF, retryDelaySupplier.calcBackoff(feedRequest2));
    }

}