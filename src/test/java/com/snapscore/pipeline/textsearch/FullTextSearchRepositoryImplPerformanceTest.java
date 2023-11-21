package com.snapscore.pipeline.textsearch;

import com.snapscore.pipeline.logging.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.function.Predicate;

public class FullTextSearchRepositoryImplPerformanceTest {

    private static final Logger log = Logger.setup(FullTextSearchRepositoryImplPerformanceTest.class);
    private final HashSet<String> wordSet = new HashSet<>();

    @Before
    public void setUp() {
        try {
            List<String> dictionaryEntries = FileUtils.readFileLinesFromResourcesDir("dictionary.txt");
            for (String dictionaryEntry : dictionaryEntries) {
                wordSet.add(dictionaryEntry.toUpperCase());
            }
        } catch (Exception e) {
            log.error("Error initialising dictionary data from file!", e);
        }
    }

    @Ignore
    @Test
    public void testQueryingBigCache() {

        FullTextSearchRepository<TestItem> itemsCache = new FullTextSearchRepositoryImpl<>("itemsCache");

        long start = System.currentTimeMillis();

        String previousWord = null;
        Random random = new Random(10);
        random.nextInt(10);

        int idx = 0;

//        int wordLimit = 100; // -1 for no limit
        int wordLimit = -1; // -1 for no limit

        for (int multiplier = 0; multiplier < 1; multiplier++) {
            int wordNo = 0;
            for (String word : wordSet) {

                // check word limit
                wordNo++;
                if (wordLimit != -1) {
                    if (wordNo > wordLimit) {
                        break;
                    }
                }

                // create item
                TestItem testItem = null;
                if (previousWord != null) {
                    testItem = new TestItem(word + " " + previousWord, String.valueOf(idx++));
                } else {
                    testItem = new TestItem(word, String.valueOf(idx++));
                }
                itemsCache.addItem(testItem);
                if (random.nextInt(10) % 9 == 0) {
                    previousWord = word;
                }
            }
        }

        int cacheSize = wordLimit > -1 ? wordLimit : wordSet.size();

        long end = System.currentTimeMillis();
        System.out.println("Fill duration: " + (end - start));

        List<String> searchTexts = List.of("word an", "word ap", "abiezer", "abiezer ha", "Aaron");
        runCacheQueries(itemsCache, 100, searchTexts);

        start = System.currentTimeMillis();
        int searchCount = 100000;
        List<Integer> word = runCacheQueries(itemsCache, searchCount, searchTexts);
        end = System.currentTimeMillis();
        System.out.println("Query duration: " + (end - start) + " ms for " + searchCount + " searches through cache of size " + cacheSize);
        System.out.println("Sum of found counts: " + word.stream().mapToInt(i -> i).sum());
    }


    private List<Integer> runCacheQueries(FullTextSearchRepository<TestItem> itemsCache, int times, List<String> searchTexts) {
        List<Integer> foundCounts = new ArrayList<>();
        for (String searchText : searchTexts) {
            for (int i = 0; i < times / searchTexts.size(); i++) {
                List<TestItem> matchingItems = itemsCache.findMatchingItems(searchText, 25, Predicate.isEqual(true));
                foundCounts.add(matchingItems.size());
            }
        }
        return foundCounts;
    }


    private static class TestItem implements FullTextSearchableItem {

        private final String name;
        private final String id;

        public TestItem(String name, String id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public List<String> getSearchableNames() {
            return List.of(name);
        }

        @Override
        public String getIdentifier() {
            return id;
        }
    }


}
