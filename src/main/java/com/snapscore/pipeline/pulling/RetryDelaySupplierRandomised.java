package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;

import java.time.Duration;
import java.util.Random;

public class RetryDelaySupplierRandomised implements RetryDelaySupplier {

    private static final Logger logger = Logger.setup(RetryDelaySupplierRandomised.class);

    private final long minRetryDelayMillis;
    private final Duration minRetryDelay;
    private static final Random RANDOM = new Random();

    public RetryDelaySupplierRandomised(long minRetryDelayMillis) {
        this.minRetryDelayMillis = minRetryDelayMillis;
        this.minRetryDelay = Duration.ofMillis(minRetryDelayMillis);
    }

    @Override
    public Duration calcBackoff(FeedRequest feedRequest) {
        Duration pullInterval = DEFAULT_FIRST_RETRY_BACKOFF;
        if (feedRequest instanceof FeedRequestWithInterval) {
            pullInterval = ((FeedRequestWithInterval) feedRequest).getPullInterval();
        }
        double root = Math.pow(pullInterval.toMillis(), 0.5);
        int randomMultiplier = RANDOM.nextInt(20) + 1;
        long delay = (long) (root * (double) randomMultiplier);
        if (delay < minRetryDelayMillis) {
            return minRetryDelay;
        } else {
            return Duration.ofMillis(delay);
        }
    }

}
