package com.snapscore.pipeline.textsearch;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class FullTextSearchRepositoryImplTest {

    private TestTeam team1;
    private TestTeam team2;
    private TestTeam team3;
    private TestTeam team4;
    private TestTeam team5;

    private Predicate<FullTextSearchableItem> predicateTrue = p -> true;

    @Before
    public void setUp() throws Exception {
        team1 = new TestTeam("1", "Alfa");
        team2 = new TestTeam("2", "Alb");
        team3 = new TestTeam("3", "American team");
        team4 = new TestTeam("4", "American tornado", true);
        team5 = new TestTeam("5", "America sucks");
    }


    @Test
    public void addItem() {
        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestFullTextSearchRepo");
        trieCache.addItem(team1);
        trieCache.addItem(team2);
        trieCache.addItem(team3);

        List<TestTeam> matchingItems = trieCache.findMatchingItems("Al", 100, predicateTrue);
        matchingItems.sort(Comparator.comparing(TestTeam::getIdentifier));
        assertEquals(List.of(team1, team2), matchingItems);
    }


    @Test
    public void removeItem() {
        // given
        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestFullTextSearchRepo");
        addAllTeams(trieCache);

        trieCache.removeItem(team2);

        List<TestTeam> matchingItems = trieCache.findMatchingItems("American", 100, predicateTrue);
        assertEquals(List.of(team3, team4), matchingItems);

        // when
        trieCache.removeItem(team3);

        // then
        matchingItems = trieCache.findMatchingItems("American", 100, predicateTrue);
        assertEquals(List.of(team4), matchingItems);

        matchingItems = trieCache.findMatchingItems("team", 100, predicateTrue);
        assertEquals(Collections.EMPTY_LIST, matchingItems);

        matchingItems = trieCache.findMatchingItems("America", 100, predicateTrue);
        assertEquals(List.of(team5, team4), matchingItems);
    }

    @Test
    public void removeItemById() {
        // given
        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestFullTextSearchRepo");
        addAllTeams(trieCache);

        trieCache.removeItemById(team2.getIdentifier());

        List<TestTeam> matchingItems = trieCache.findMatchingItems("American", 100, predicateTrue);
        assertEquals(List.of(team3, team4), matchingItems);

        // when
        trieCache.removeItemById(team3.getIdentifier());

        // then
        matchingItems = trieCache.findMatchingItems("American", 100, predicateTrue);
        assertEquals(List.of(team4), matchingItems);

        matchingItems = trieCache.findMatchingItems("team", 100, predicateTrue);
        assertEquals(Collections.EMPTY_LIST, matchingItems);

        matchingItems = trieCache.findMatchingItems("America", 100, predicateTrue);
        assertEquals(List.of(team5, team4), matchingItems);
    }

    @Test
    public void findMatchingItemsForMultiWordInput() {

        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestFullTextSearchRepo");
        addAllTeams(trieCache);

        List<TestTeam> matchingItems = trieCache.findMatchingItems("Ame te", 100, predicateTrue);
        assertEquals(List.of(team3), matchingItems);

        matchingItems = trieCache.findMatchingItems("Ame t", 100, predicateTrue);
        assertEquals(List.of(team3, team4), matchingItems);

        matchingItems = trieCache.findMatchingItems("t Ame", 100, predicateTrue);
        assertEquals(List.of(team3, team4), matchingItems);
    }

    @Test
    public void findMatchingItemsExcludePredicate() {
        FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestFullTextSearchRepo");
        trieCache.addItem(team3);
        trieCache.addItem(team4);

        Predicate<FullTextSearchableItem> predicate = p -> !((TestTeam) p).placeholder;

        List<TestTeam> collect = List.of(team3).stream().filter(predicate).collect(Collectors.toList());
        assertEquals(1, collect.size());

        List<TestTeam> matchingItems = trieCache.findMatchingItems("Ame", 100, predicate);
        assertEquals(1, matchingItems.size());
    }

    @Test
    public void testFindingTeamBySynonymNameWhenDictionaryEntryHasIdentifier() {

        final SynonymsDictionary synonymsDictionary = new SynonymsDictionaryImpl();

        final String synonym1 = "AMTO";
        final String synonym2 = "AT";
        final List<String> synonyms = List.of(synonym1, synonym2);

        final SynonymsEntry synonymsForTeam4 = new SynonymsEntry(team4.getIdentifier(), synonyms);

        synonymsDictionary.setEntry(synonymsForTeam4);

        final FullTextSearchableItemFactory<TestTeam> fullTextSearchableItemFactory = TestTeam::new;
        final FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestFullTextSearchRepo", synonymsDictionary, fullTextSearchableItemFactory);

        addAllTeams(trieCache);

        assertItemFoundForSynonym(synonym1, team4, trieCache);
        assertItemFoundForSynonym(synonym2, team4, trieCache);
    }

    @Test
    public void testFindingTeamBySynonymNameWhenDictionaryEntryHasNoIdentifier() {

        final SynonymsDictionary synonymsDictionary = new SynonymsDictionaryImpl();

        final String teamName = team4.names.get(0);
        final String synonym1 = "AMTO";
        final String synonym2 = "AT";
        final List<String> synonyms = List.of(teamName, synonym1, synonym2);

        final SynonymsEntry synonymsForTeam4 = new SynonymsEntry(null, synonyms);

        synonymsDictionary.setEntry(synonymsForTeam4);

        final FullTextSearchableItemFactory<TestTeam> fullTextSearchableItemFactory = TestTeam::new;
        final FullTextSearchRepositoryImpl<TestTeam> trieCache = new FullTextSearchRepositoryImpl<>("TestFullTextSearchRepo", synonymsDictionary, fullTextSearchableItemFactory);

        addAllTeams(trieCache);

        assertItemFoundForSynonym(synonym1, team4, trieCache);
        assertItemFoundForSynonym(synonym2, team4, trieCache);
    }

    private void assertItemFoundForSynonym(String synonym, TestTeam expectedItem, FullTextSearchRepositoryImpl<TestTeam> trieCache) {
        final List<TestTeam> matchingItems = trieCache.findMatchingItems(synonym, 100, predicateTrue);
        assertEquals(1, matchingItems.size());
        assertEquals(expectedItem.getIdentifier(), matchingItems.get(0).getIdentifier());
    }

    private void addAllTeams(FullTextSearchRepositoryImpl<TestTeam> trieCache) {
        trieCache.addItem(team1);
        trieCache.addItem(team2);
        trieCache.addItem(team3);
        trieCache.addItem(team4);
        trieCache.addItem(team5);
    }
}
