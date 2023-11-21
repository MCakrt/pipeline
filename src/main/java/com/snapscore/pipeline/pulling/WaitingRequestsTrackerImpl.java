package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public class WaitingRequestsTrackerImpl implements WaitingRequestsTracker {

    private static final Logger logger = Logger.setup(ScheduledPullingCacheImpl.class);

    private final Function<FeedRequest, String> deduplicatingFeedRequestKeyMaker;
    private final ConcurrentMap<String, TrackedRequest> requestsAwaitingToBePulledByUrlMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TrackedRequest> requestsAwaitingToBeRetriedByUrlMap = new ConcurrentHashMap<>();

    /**
     * @param deduplicatingFeedRequestKeyMaker must return a string that will be used as an identifier
     *                                         for the tracked request and to filter out duplicate requests
     */
    public WaitingRequestsTrackerImpl(Function<FeedRequest, String> deduplicatingFeedRequestKeyMaker) {
        this.deduplicatingFeedRequestKeyMaker = deduplicatingFeedRequestKeyMaker;
    }

    @Override
    public boolean isAwaitingResponse(FeedRequest feedRequest) {
        return isAwaiting(feedRequest, requestsAwaitingToBePulledByUrlMap);
    }

    @Override
    public boolean isAwaitingRetry(FeedRequest feedRequest) {
        return isAwaiting(feedRequest, requestsAwaitingToBeRetriedByUrlMap);
    }

    private boolean isAwaiting(FeedRequest feedRequest, ConcurrentMap<String, TrackedRequest> urlMap) {
        try {
            return urlMap.containsKey(makeKey(feedRequest));
        } catch (Exception e) {
            logger.error("Error! {}", feedRequest, e);
            return false;
        }
    }

    @Override
    public void trackAwaitingResponse(FeedRequest feedRequest) {
        trackAwaiting(feedRequest, requestsAwaitingToBePulledByUrlMap);
    }

    @Override
    public void trackAwaitingRetry(FeedRequest feedRequest) {
        trackAwaiting(feedRequest, requestsAwaitingToBeRetriedByUrlMap);
    }

    private void trackAwaiting(FeedRequest feedRequest, ConcurrentMap<String, TrackedRequest> urlMap) {
        try {
            urlMap.put(makeKey(feedRequest), new TrackedRequest(feedRequest));
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid()).analyticsId("tracking_request")).info("Tracking feedRequest: {}", feedRequest.toStringBasicInfo());
        } catch (Exception e) {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error while tracking request! {}", feedRequest, e);
        }
    }

    @Override
    public Optional<TrackedRequest> getTrackedRequest(FeedRequest feedRequest) {
        TrackedRequest trackedRequest = requestsAwaitingToBePulledByUrlMap.get(makeKey(feedRequest));
        return Optional.ofNullable(trackedRequest);
    }

    @Override
    public void untrackProcessed(FeedRequest feedRequest) {
        untrack(feedRequest, requestsAwaitingToBePulledByUrlMap);
    }

    @Override
    public void untrackRetried(FeedRequest feedRequest) {
        untrack(feedRequest, requestsAwaitingToBeRetriedByUrlMap);
    }

    private void untrack(FeedRequest feedRequest, ConcurrentMap<String, TrackedRequest> urlMap) {
        try {
            TrackedRequest removedRq = urlMap.remove(makeKey(feedRequest));
            if (removedRq == null) {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).warn("FeedRequest not present in the tracker but it should be - nothing to untrack: {}", feedRequest.toStringBasicInfo());
            } else {
                logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).info("Untracked feedRequest: {}", feedRequest.toStringBasicInfo());
            }
        } catch (Exception e) {
            logger.decorateSetup(mdc -> mdc.anyId(feedRequest.getUuid())).error("Error while untracking request! {}", feedRequest, e);
        }
    }

    @Override
    public int countOfRequestsAwaitingResponse() {
        return requestsAwaitingToBePulledByUrlMap.size();
    }

    private String makeKey(FeedRequest feedRequest) {
        try {
            return deduplicatingFeedRequestKeyMaker.apply(feedRequest);
        } catch (Exception e) {
            logger.error("Error while making feedRequest key! Will use request url! {}", feedRequest, e);
            return feedRequest.getUrl();
        }
    }

}
