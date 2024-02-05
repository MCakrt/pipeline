package com.snapscore.pipeline.pulling;


import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FeedRequestWithInterval extends FeedRequest {

    private final Duration pullInterval;

    public FeedRequestWithInterval(FeedName feedName,
                                   String url,
                                   FeedPriorityEnum priority,
                                   Duration pullInterval,
                                   int numOfRetries,
                                   FeedRequestProperties properties,
                                   RetryDelaySupplier retryDelaySupplier,
                                   List<FeedRequestHttpHeader> httpHeaders) {
        super(feedName, url, priority, numOfRetries, properties, retryDelaySupplier, httpHeaders);
        this.pullInterval = pullInterval;
    }

    public FeedRequestWithInterval(FeedName feedName,
                                   String url,
                                   String urlForLogging,
                                   FeedPriorityEnum priority,
                                   Duration pullInterval,
                                   int numOfRetries,
                                   FeedRequestProperties properties,
                                   RetryDelaySupplier retryDelaySupplier,
                                   List<FeedRequestHttpHeader> httpHeaders) {
        super(feedName, url, urlForLogging, priority, numOfRetries, properties, retryDelaySupplier, httpHeaders);
        this.pullInterval = pullInterval;
    }

    public static FeedRequestWithIntervalBuilder newBuilder(FeedName feedName,
                                                            FeedPriorityEnum priority,
                                                            int numOfRetries,
                                                            String url,
                                                            Duration pullInterval) {
        return new FeedRequestWithIntervalBuilder(feedName, priority, numOfRetries, url, pullInterval);
    }


    public Duration getPullInterval() {
        return pullInterval;
    }

    public Duration getRetryBackoff() {
        if (retryDelaySupplier != null) {
            return retryDelaySupplier.calcBackoff(this);
        } else {
            return RetryDelaySupplier.DEFAULT_FIRST_RETRY_BACKOFF;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeedRequestWithInterval)) return false;
        if (!super.equals(o)) return false;
        FeedRequestWithInterval that = (FeedRequestWithInterval) o;
        return Objects.equals(pullInterval, that.pullInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pullInterval);
    }

    public String toStringBasicInfo() {
        return "[uuid='" + uuid + '\'' +
                ", url='" + urlForLogging + '\'' +
                ", interval=" + pullInterval +
                ']'
                ;
    }

    @Override
    public String toString() {
        return "FeedRequestWithInterval{" +
                "pullInterval=" + pullInterval +
                ", feedName=" + feedName +
                ", priority=" + priority +
                ", numOfRetries=" + numOfRetries +
                ", uuid='" + uuid + '\'' +
                ", url='" + urlForLogging + '\'' +
                ", createdDt=" + createdDt +
                ", httpHeaders=" + httpHeaders +
                ", retryDelaySupplier=" + retryDelaySupplier +
                ", properties=" + properties +
                '}';
    }

    public static class FeedRequestWithIntervalBuilder {

        private FeedName feedName;
        private FeedPriorityEnum priority;
        private int numOfRetries;
        private String url;
        private String urlForLogging;
        private FeedRequestProperties properties;
        private RetryDelaySupplier retryDelaySupplier;
        private Duration pullInterval;
        private List<FeedRequestHttpHeader> headers = new ArrayList<>();

        FeedRequestWithIntervalBuilder(FeedName feedName, FeedPriorityEnum priority, int numOfRetries, String url, Duration pullInterval) {
            this.feedName = feedName;
            this.priority = priority;
            this.numOfRetries = numOfRetries;
            this.url = url;
            this.properties = new FeedRequestProperties();
            this.pullInterval = pullInterval;
        }

        public FeedRequestWithIntervalBuilder setFeedName(FeedName feedName) {
            this.feedName = feedName;
            return this;
        }

        public FeedRequestWithIntervalBuilder setPriority(FeedPriorityEnum priority) {
            this.priority = priority;
            return this;
        }

        public FeedRequestWithIntervalBuilder setNumOfRetries(int numOfRetries) {
            this.numOfRetries = numOfRetries;
            return this;
        }

        public FeedRequestWithIntervalBuilder setUrl(String url) {
            this.url = url;
            return this;
        }

        public FeedRequestWithIntervalBuilder setUrlForLogging(String urlForLogging) {
            this.urlForLogging = urlForLogging;
            return this;
        }

        public FeedRequestWithIntervalBuilder putHeader(String key, String value) {
            this.headers.add(new FeedRequestHttpHeader(key, value));
            return this;
        }


        public FeedRequestWithIntervalBuilder putProperty(Enum propertyType, Object value) {
            this.properties.putProperty(propertyType, value);
            return this;
        }

        public FeedRequestWithIntervalBuilder setRetryDelaySupplier(RetryDelaySupplier retryDelaySupplier) {
            this.retryDelaySupplier = retryDelaySupplier;
            return this;
        }

        public FeedRequestWithIntervalBuilder setPullInterval(Duration pullInterval) {
            this.pullInterval = pullInterval;
            return this;
        }

        public FeedRequestWithInterval build() {
            final String urlForLogging = this.urlForLogging != null ? this.urlForLogging : url;
            return new FeedRequestWithInterval(feedName, url, urlForLogging, priority, pullInterval, numOfRetries,
                    properties, retryDelaySupplier, headers);
        }

    }
}
