package com.snapscore.pipeline.textsearch;

import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class SynonymsDictionaryImplTest {

    @Test
    public void getEntries() {

        final SynonymsDictionary synonymsDictionary = new SynonymsDictionaryImpl();

        final String id = "123";
        final String name = "France";
        final List<String> synonyms = List.of(name, "Fr", "Fra", "Fce");

        final SynonymsEntry entry1 = new SynonymsEntry(id, synonyms);

        synonymsDictionary.setEntry(entry1);

        assertEquals(Optional.of(entry1), synonymsDictionary.getEntryByEntityId(id));
        assertEquals(Optional.of(entry1), synonymsDictionary.getEntryByName("Fce"));

        final TestTeam testTeam = new TestTeam(id, name);
        assertEquals(List.of(entry1), synonymsDictionary.getEntriesBySearchableItem(testTeam));
    }


    @Test
    public void addEntries() {
        final SynonymsDictionary synonymsDictionary = new SynonymsDictionaryImpl();

        final String id = "123";
        final String name = "France";
        final List<String> synonyms1 = List.of(name, "Fce");
        final SynonymsEntry entry1 = new SynonymsEntry(id, synonyms1);

        synonymsDictionary.addEntry(entry1);

        final List<String> synonyms2 = List.of(name, "Fce_2");
        final SynonymsEntry entry2 = new SynonymsEntry(id, synonyms2);

        synonymsDictionary.addEntry(entry2);

        assertEquals(3, synonymsDictionary.getEntryByEntityId(id).get().synonyms().size());

    }

}
