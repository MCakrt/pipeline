package com.snapscore.pipeline.textsearch;

import java.util.Collection;

public interface FullTextSearchableItem {

    /**
     * @return the name by which this item can be looked up; The user of this should not perform any splitting of the names by spaces. This is handled by the library.
     * Example: for the stage "UEFA" we might want to return a list like this ["UEFA", "UEFA Champions League"]
     */
    Collection<String> getSearchableNames();

    /**
     * @return unique item identifier by which the item can be identifier when it needs to be removed from the TrieBasedCache
     * IMPORTANT: can return null, client code needs to check !!!
     */
    String getIdentifier();

}
