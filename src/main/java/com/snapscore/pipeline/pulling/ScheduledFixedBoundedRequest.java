package com.snapscore.pipeline.pulling;

import reactor.core.Disposable;

/**
 * encapsulates request that is sent a bounded number of times
 */
public class ScheduledFixedBoundedRequest<K> extends ScheduledPulling<K> {

    private final FeedRequestWithInterval feedRequest;
    private final int repeatedTimes;

    public ScheduledFixedBoundedRequest(K scheduledPullingKey,
                                        FeedRequestWithInterval feedRequest,
                                        Disposable disposable,
                                        int repeatedTimes) {
        super(scheduledPullingKey, disposable, feedRequest.getPullInterval());
        this.feedRequest = feedRequest;
        this.repeatedTimes = repeatedTimes;
    }

    public FeedRequest getFeedRequest() {
        return feedRequest;
    }

    public int getRepeatedTimes() {
        return repeatedTimes;
    }
}
