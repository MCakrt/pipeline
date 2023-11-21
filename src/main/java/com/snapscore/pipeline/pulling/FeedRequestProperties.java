package com.snapscore.pipeline.pulling;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains additional details about the request, that is useful for e.g. making the right file name ...
 */
public class FeedRequestProperties {

    private final Map<Enum<?>, Object> properties = new ConcurrentHashMap<>(2);

    public FeedRequestProperties() {
    }

    void putProperty(Enum<?> propertyType, Object value) {
        if (propertyType != null && value != null) {
            properties.put(propertyType, value);
        }
    }

    public <V> Optional<V> getPropertyValue(Enum<?> propertyType, Class<V> valueType) {
        Object prop = properties.get(propertyType);
        if (prop != null) {
            V cast = valueType.cast(prop);
            return Optional.of(cast);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FeedRequestProperties)) return false;
        FeedRequestProperties that = (FeedRequestProperties) o;
        return Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(properties);
    }

    @Override
    public String toString() {
        return "FeedRequestProperties{" +
                "properties=" + properties +
                '}';
    }
}
