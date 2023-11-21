package com.snapscore.pipeline.textsearch;

import com.snapscore.pipeline.logging.Logger;
import reactor.util.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record SynonymsEntry(@Nullable String entityId, List<String> synonyms) {

    private static final Logger log = Logger.setup(SynonymsEntry.class);

    public Optional<SynonymsEntry> merge(SynonymsEntry other) {
        final List<String> mergedSynonyms = Stream.concat(synonyms.stream(), other.synonyms.stream()).distinct().collect(Collectors.toList());
        if (Objects.equals(entityId, other.entityId)) {
            return Optional.of(new SynonymsEntry(entityId, mergedSynonyms));
        } else if (entityId == null || other.entityId == null) {
            String iden = entityId != null ? entityId : other.entityId;
            return Optional.of(new SynonymsEntry(iden, mergedSynonyms));
        } else {
            log.error("Cannot merge two entries with different identifiers: {}, {}", this, other);
            return Optional.empty();
        }
    }

}
