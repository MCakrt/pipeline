package com.snapscore.pipeline.pulling;

import reactor.core.Disposable;

import java.time.Duration;

public abstract class ScheduledPulling<K> {

    private final K scheduledPullingKey;
    private final Disposable disposable;
    private final Duration pullInterval;

    /**
     * @param scheduledPullingKey must have properly implemented equals() and hashCode() methods
     */
    public ScheduledPulling(K scheduledPullingKey, Disposable disposable, Duration pullInterval) {
        this.scheduledPullingKey = scheduledPullingKey;
        this.disposable = disposable;
        this.pullInterval = pullInterval;
    }

    public K getScheduledPullingKey() {
        return scheduledPullingKey;
    }

    public void cancel() {
        disposable.dispose();
    }

    public boolean isCancelled() {
        return disposable.isDisposed();
    }

    public Duration getPullInterval() {
        return pullInterval;
    }

    public Disposable getDisposable() {
        return disposable;
    }
}
