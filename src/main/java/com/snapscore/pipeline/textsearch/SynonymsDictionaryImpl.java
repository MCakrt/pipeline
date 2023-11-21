package com.snapscore.pipeline.textsearch;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SynonymsDictionaryImpl implements SynonymsDictionary {

    private final ConcurrentMap<String, SynonymsEntry> entriesByIds = new ConcurrentHashMap<>();

    // key must be UPPER CASE
    private final ConcurrentMap<String, SynonymsEntry> entriesBySynonymNames = new ConcurrentHashMap<>();

    @Override
    public void addEntry(SynonymsEntry entry) {
        if (entry.entityId() != null) {
            final Optional<SynonymsEntry> prevEntry = getEntryByEntityId(entry.entityId());
            if (prevEntry.isPresent()) {
                entriesByIds.put(entry.entityId(), prevEntry.get().merge(entry).orElse(prevEntry.get()));
            } else {
                entriesByIds.put(entry.entityId(), entry);
            }
        }
        if (entry.synonyms() != null) {
            for (String synonym : entry.synonyms()) {
                final SynonymsEntry mergedEntry = getEntryByName(synonym).flatMap(prev -> prev.merge(entry)).orElse(entry);
                addMappingBySynonyms(mergedEntry);
            }
        }
    }

    @Override
    public void setEntry(SynonymsEntry entry) {
        if (entry.entityId() != null) {
            entriesByIds.put(entry.entityId(), entry);
        }
        if (entry.synonyms() != null) {
            // remove previous mapping
            entry.synonyms().stream().flatMap(syn -> getEntryByName(syn).stream()).forEach(this::removeEntry);
            addMappingBySynonyms(entry);
        }
    }

    private void addMappingBySynonyms(SynonymsEntry entry) {
        // add new mapping
        for (String synonym : entry.synonyms()) {
            entriesBySynonymNames.put(synonym.toUpperCase(), entry);
        }
    }

    @Override
    public void removeEntry(SynonymsEntry entry) {
        entriesByIds.remove(entry.entityId());
        for (String synonym : entry.synonyms()) {
            entriesBySynonymNames.remove(synonym.toUpperCase());
        }
    }

    @Override
    public Optional<SynonymsEntry> getEntryByName(String name) {
        return Optional.ofNullable(entriesBySynonymNames.get(name.toUpperCase()));
    }

    @Override
    public Optional<SynonymsEntry> getEntryByEntityId(String entityId) {
        return Optional.ofNullable(entriesByIds.get(entityId));
    }

    @Override
    public List<SynonymsEntry> getEntriesBySearchableItem(FullTextSearchableItem searchableItem) {
        final Stream<SynonymsEntry> entriesForSearchableNames = searchableItem.getSearchableNames().stream().flatMap(name -> getEntryByName(name).stream());
        final Stream<SynonymsEntry> entryForId = getEntryByEntityId(searchableItem.getIdentifier()).stream();
        return Stream.concat(entriesForSearchableNames, entryForId).distinct().collect(Collectors.toList());
    }

}
