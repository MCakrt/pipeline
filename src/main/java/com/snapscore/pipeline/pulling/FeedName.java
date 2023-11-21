package com.snapscore.pipeline.pulling;

import java.util.Objects;

/**
 * Encapsulates the enum value of an enum for the feedName.
 * For example if the provider implementation pulls 3 different types of feeds/urls for data of stages, stage events, and event details
 * then the provider implementation should define an enum for these three (e.g. STAGES_FEED, STAGE_EVENTS_FEED, EVENT_DETAILS_FEED) and each of them will have a feed url template mapped to them.
 * FeedName then takes in one such value identifying the name of the feed.
 */
public class FeedName {

    private final Enum<?> name;

    public FeedName(Enum<?> name) {
        this.name = name;
    }

    public Enum<?> getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeedName)) return false;
        FeedName feedName = (FeedName) o;
        return Objects.equals(name, feedName.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "FeedName{" +
                "name=" + name +
                '}';
    }
}
