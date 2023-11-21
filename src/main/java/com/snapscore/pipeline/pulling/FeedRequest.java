package com.snapscore.pipeline.pulling;


import com.snapscore.pipeline.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class is not meant to be extended directly. Instead, additional properties can be passed through FeedRequestProperties
 */
public class FeedRequest {

    public static final Comparator<FeedRequest> DEFAULT_PRIORITY_COMPARATOR = Comparator.comparingInt(FeedRequest::getSchedulingOrder).thenComparing(FeedRequest::getCreatedDt);

    final FeedName feedName;
    final FeedPriorityEnum priority;
    final int numOfRetries;
    final String uuid;
    final String url;
    /**
     * URL to be used in logs. Use this to hide sensitive information from the urls, such as passwords
     * If not set, the url property will be used
     */
    final String urlForLogging;
    final LocalDateTime createdDt; // UTC
    final Map<String, FeedRequestHttpHeader> httpHeaders;
    final RetryDelaySupplier retryDelaySupplier;
    final FeedRequestProperties properties;

    FeedRequest(FeedName feedName,
                String url,
                FeedPriorityEnum priority,
                int numOfRetries,
                FeedRequestProperties properties,
                RetryDelaySupplier retryDelaySupplier,
                List<FeedRequestHttpHeader> httpHeaders) {
        this(feedName, url, url, priority, numOfRetries, properties, retryDelaySupplier, httpHeaders);
    }

    FeedRequest(FeedName feedName,
                String url,
                String urlForLogging,
                FeedPriorityEnum priority,
                int numOfRetries,
                FeedRequestProperties properties,
                RetryDelaySupplier retryDelaySupplier,
                List<FeedRequestHttpHeader> httpHeaders) {

        this.feedName = feedName;
        this.priority = priority;
        this.numOfRetries = numOfRetries;
        String uuidFeedName = feedName != null ? feedName.getName().name() : "";
        this.uuid = UUID.randomUUID().toString().toUpperCase().replaceAll("-", "").substring(0, 10) + "_" + uuidFeedName;
        this.url = url;
        this.urlForLogging = urlForLogging;
        this.retryDelaySupplier = retryDelaySupplier;
        this.createdDt = DateUtils.nowUTC();
        this.httpHeaders = new ConcurrentHashMap<>();
        if (httpHeaders != null) {
            for (FeedRequestHttpHeader httpHeader : httpHeaders) {
                if (httpHeader != null && httpHeader.getKey() != null) {
                    this.httpHeaders.put(httpHeader.getKey(), httpHeader);
                }
            }
        }
        this.properties = properties;
    }

    public static FeedRequestBuilder newBuilder(FeedName feedName,
                                                FeedPriorityEnum priority,
                                                int numOfRetries,
                                                String url) {
        return new FeedRequestBuilder(feedName, priority, numOfRetries, url);
    }


    public FeedName getFeedName() {
        return feedName;
    }

    public FeedPriorityEnum getPriority() {
        return priority;
    }

    public int getNumOfRetries() {
        return numOfRetries;
    }

    public String getUuid() {
        return uuid;
    }

    public String getUrl() {
        return url;
    }

    public String getUrlForLogging() {
        return urlForLogging;
    }

    public LocalDateTime getCreatedDt() {
        return createdDt;
    }

    public Collection<FeedRequestHttpHeader> getHttpHeaders() {
        return Collections.unmodifiableCollection(httpHeaders.values());
    }

    public FeedRequestProperties getProperties() {
        return properties;
    }

    public RetryDelaySupplier getRetryDelaySupplier() {
        return retryDelaySupplier;
    }

    public Duration getRetryBackoff() {
        if (retryDelaySupplier != null) {
            return retryDelaySupplier.calcBackoff(this);
        } else {
            return RetryDelaySupplier.DEFAULT_FIRST_RETRY_BACKOFF;
        }
    }

    public int getSchedulingOrder() {
        return priority.getSchedulingOrder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeedRequest)) return false;
        FeedRequest that = (FeedRequest) o;
        return numOfRetries == that.numOfRetries &&
                Objects.equals(feedName, that.feedName) &&
                priority == that.priority &&
                Objects.equals(uuid, that.uuid) &&
                Objects.equals(url, that.url) &&
                Objects.equals(createdDt, that.createdDt) &&
                Objects.equals(httpHeaders, that.httpHeaders) &&
                Objects.equals(retryDelaySupplier, that.retryDelaySupplier) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(feedName, priority, numOfRetries, uuid, url, createdDt, httpHeaders, retryDelaySupplier, properties);
    }

    public String toStringBasicInfo() {
        return "[uuid='" + uuid + '\'' +
                ", url='" + urlForLogging + '\'' +
                ']'
                ;
    }

    @Override
    public String toString() {
        return "FeedRequest{" +
                "feedName=" + feedName +
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

    public static class FeedRequestBuilder {

        private FeedName feedName;
        private FeedPriorityEnum priority;
        private int numOfRetries;
        private String url;
        private String urlForLogging;
        private FeedRequestProperties properties;
        private RetryDelaySupplier retryDelaySupplier;
        private List<FeedRequestHttpHeader> headers = new ArrayList<>();

        <T extends Enum<T>, S extends Enum<S>> FeedRequestBuilder(Enum<T> feedNameEnum,
                                                                  Enum<S> feedDataTypeEnum,
                                                                  FeedPriorityEnum priority,
                                                                  int numOfRetries,
                                                                  String url) {
            this(new FeedName(feedNameEnum), priority, numOfRetries, url);
        }

        FeedRequestBuilder(FeedName feedName,
                           FeedPriorityEnum priority,
                           int numOfRetries,
                           String url) {
            this.feedName = feedName;
            this.priority = priority;
            this.numOfRetries = numOfRetries;
            this.url = url;
            this.properties = new FeedRequestProperties();
        }

        public FeedRequestBuilder setFeedName(FeedName feedName) {
            this.feedName = feedName;
            return this;
        }

        public FeedRequestBuilder setPriority(FeedPriorityEnum priority) {
            this.priority = priority;
            return this;
        }

        public FeedRequestBuilder setNumOfRetries(int numOfRetries) {
            this.numOfRetries = numOfRetries;
            return this;
        }

        public FeedRequestBuilder setUrl(String url) {
            this.url = url;
            return this;
        }

        public FeedRequestBuilder setUrlForLogging(String urlForLogging) {
            this.urlForLogging = urlForLogging;
            return this;
        }

        public FeedRequestBuilder putProperty(Enum<?> propertyType, Object value) {
            this.properties.putProperty(propertyType, value);
            return this;
        }

        public FeedRequestBuilder putHeader(String key, String value) {
            this.headers.add(new FeedRequestHttpHeader(key, value));
            return this;
        }


        public FeedRequestBuilder setRetryDelaySupplier(RetryDelaySupplier retryDelaySupplier) {
            this.retryDelaySupplier = retryDelaySupplier;
            return this;
        }

        public FeedRequest build() {
            final String urlForLogging = this.urlForLogging != null ? this.urlForLogging : url;
            return new FeedRequest(feedName, url, urlForLogging, priority, numOfRetries, properties, retryDelaySupplier, headers);
        }
    }
}
