package com.snapscore.pipeline.textsearch;

import java.util.List;
import java.util.Optional;

public interface SynonymsDictionary {

    /**
     * If there is an existing entry for the same id/synonyms it gets merged with the one specified
     */
    void addEntry(SynonymsEntry entry);

    /**
     * If there is an existing entry for the same id/synonyms it gets replaced by the one provided here
     */
    void setEntry(SynonymsEntry entry);

    void removeEntry(SynonymsEntry entry);

    /**
     * @param name  One of the synonym names
     */
    Optional<SynonymsEntry> getEntryByName(String name);

    Optional<SynonymsEntry> getEntryByEntityId(String entityId);

    List<SynonymsEntry> getEntriesBySearchableItem(FullTextSearchableItem searchableItem);

}
