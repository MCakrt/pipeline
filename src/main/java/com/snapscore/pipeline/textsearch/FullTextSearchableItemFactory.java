package com.snapscore.pipeline.textsearch;

import java.util.Collection;

/**
 * Used for creating new instances of a specific implementation of {@link FullTextSearchableItem}.
 */
@FunctionalInterface
public interface FullTextSearchableItemFactory<T extends FullTextSearchableItem> {

    T from(String identifier, Collection<String> searchableNames);

}
