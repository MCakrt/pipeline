package com.snapscore.pipeline.pulling;

import com.snapscore.pipeline.logging.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

public class ScheduledPullingCacheImpl<K, V extends ScheduledPulling<K>> implements ScheduledPullingCache<K, V> {

    private static final Logger logger = Logger.setup(ScheduledPullingCacheImpl.class);

    private final Map<K, V> scheduledPullingsMap = new ConcurrentHashMap<>();

    @Override
    public void setScheduledPulling(V scheduledPulling) {
        if (scheduledPulling != null) {
            cancelScheduledPullingOf(scheduledPulling.getScheduledPullingKey());
            scheduledPullingsMap.put(scheduledPulling.getScheduledPullingKey(), scheduledPulling);
        } else {
            logger.warn("setScheduledPulling() got null scheduledPulling!");
        }
    }

    @Override
    public Optional<V> getScheduledPulling(K scheduledPullingKey) {
        if (scheduledPullingKey != null) {
            return Optional.ofNullable(scheduledPullingsMap.get(scheduledPullingKey));
        } else {
            logger.warn("getScheduledPulling() got null scheduledPullingKey!");
            return Optional.empty();
        }
    }

    @Override
    public Optional<V> removeScheduledPulling(K scheduledPullingKey) {
        if (scheduledPullingKey != null) {
            return Optional.ofNullable(scheduledPullingsMap.remove(scheduledPullingKey));
        } else {
            logger.warn("removeScheduledPulling() got null scheduledPullingKey!");
            return Optional.empty();
        }
    }

    @Override
    public void cancelScheduledPulling(K scheduledPullingKey) {
        if (scheduledPullingKey != null) {
            cancelScheduledPullingOf(scheduledPullingKey);
        } else {
            logger.warn("cancelScheduledPulling() got null scheduledPullingKey!");
        }
    }

    private void cancelScheduledPullingOf(K scheduledPullingKey) {
        V prevScheduledPulling = scheduledPullingsMap.get(scheduledPullingKey);
        if (prevScheduledPulling != null) {
            prevScheduledPulling.cancel();
            scheduledPullingsMap.remove(scheduledPullingKey);
            logger.info("Cancelled pulling associated with key {}", scheduledPullingKey);
        }
    }

    @Override
    public List<K> cancelAllScheduledPulling(Predicate<K> cancellationCondition) {
        if (cancellationCondition != null) {
            List<K> cancelledScheduledPullingKeys = new ArrayList<>();
            Iterator<K> iterator = scheduledPullingsMap.keySet().iterator();
            while (iterator.hasNext()) {
                K nextKey = iterator.next();
                if (cancellationCondition.test(nextKey)) {
                    V nextValue = scheduledPullingsMap.get(nextKey);
                    if (nextValue != null) {
                        nextValue.cancel();
                        logger.info("Cancelled pulling associated with key {}", nextKey);
                    }
                    iterator.remove();
                    cancelledScheduledPullingKeys.add(nextKey);
                }
            }
            return cancelledScheduledPullingKeys;
        } else {
            logger.warn("cancelAllScheduledPulling() got null cancellationCondition!");
            return Collections.emptyList();
        }
    }

    @Override
    public void cancelAllScheduledPulling() {
        cancelAllScheduledPulling(key -> true);
    }

    @Override
    public boolean isPullingScheduledFor(K scheduledPullingKey) {
        if (scheduledPullingKey != null) {
            V scheduledPulling = scheduledPullingsMap.get(scheduledPullingKey);
            return scheduledPulling != null && !scheduledPulling.isCancelled();
        } else {
            logger.warn("isPullingScheduledFor() got null scheduledPullingKey!");
            return false;
        }
    }

    @Override
    public int size() {
        return scheduledPullingsMap.size();
    }
}
