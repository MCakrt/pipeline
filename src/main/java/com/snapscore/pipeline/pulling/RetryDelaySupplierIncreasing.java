package com.snapscore.pipeline.pulling;

import java.time.Duration;

public class RetryDelaySupplierIncreasing implements RetryDelaySupplier {

    /**
     * Calculate backoff delay for the given feed request.
     * Start with half the request interval and double it with every subsequent request.
     * Return {@link RetryDelaySupplier#DEFAULT_FIRST_RETRY_BACKOFF DEFAULT_FIRST_RETRY_BACKOFF} for feed requests without interval.
     *
     * @param feedRequest Feed request
     * @return Backoff delay
     */
    @Override
    public Duration calcBackoff(FeedRequest feedRequest) {
        if (feedRequest instanceof FeedRequestWithInterval feedRequestWithInterval) {
            Duration pullInterval = feedRequestWithInterval.getPullInterval();
            Duration firstRetry = pullInterval.dividedBy(2);
            return firstRetry.multipliedBy((long) Math.pow(2, feedRequest.getNumOfRetries()));
        } else {
            return DEFAULT_FIRST_RETRY_BACKOFF;
        }
    }
}
