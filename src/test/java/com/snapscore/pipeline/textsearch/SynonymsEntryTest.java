package com.snapscore.pipeline.textsearch;

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class SynonymsEntryTest {

    @Test
    public void mergeWhenOneIdentifierIsPresentForBoth() {

        final String id = "123";
        final String name = "France";
        final List<String> synonyms1 = List.of(name, "Fce");
        final SynonymsEntry entry1 = new SynonymsEntry(id, synonyms1);

        final List<String> synonyms2 = List.of(name, "Fce_2");
        final SynonymsEntry entry2 = new SynonymsEntry(id, synonyms2);

        final Optional<SynonymsEntry> mergedEntry = entry1.merge(entry2);

        assertTrue(mergedEntry.isPresent());

        assertEquals(id, mergedEntry.get().entityId());
        assertEquals(3, mergedEntry.get().synonyms().size());
    }

    @Test
    public void mergeWhenOneIdentifierIsMissingForOne() {

        final String id = "123";
        final String name = "France";
        final List<String> synonyms1 = List.of(name, "Fce");
        final SynonymsEntry entry1 = new SynonymsEntry(id, synonyms1);

        final List<String> synonyms2 = List.of(name, "Fce_2");
        final SynonymsEntry entry2 = new SynonymsEntry(null, synonyms2);

        final Optional<SynonymsEntry> mergedEntry = entry1.merge(entry2);

        assertTrue(mergedEntry.isPresent());

        assertEquals(id, mergedEntry.get().entityId());
        assertEquals(3, mergedEntry.get().synonyms().size());
    }

    @Test
    public void mergeNotDoneWhenBothIdentifiersDifferent() {

        final String id1 = "123";
        final String name = "France";
        final List<String> synonyms1 = List.of(name, "Fce");
        final SynonymsEntry entry1 = new SynonymsEntry(id1, synonyms1);

        final String id2 = "456";
        final List<String> synonyms2 = List.of(name, "Fce_2");
        final SynonymsEntry entry2 = new SynonymsEntry(id2, synonyms2);

        final Optional<SynonymsEntry> mergedEntry = entry1.merge(entry2);

        assertFalse(mergedEntry.isPresent());
    }


}
