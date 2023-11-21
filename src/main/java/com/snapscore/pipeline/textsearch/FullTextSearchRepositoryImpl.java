package com.snapscore.pipeline.textsearch;

import com.snapscore.pipeline.concurrency.LockingWrapper;
import com.snapscore.pipeline.logging.Logger;
import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import reactor.util.annotation.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class FullTextSearchRepositoryImpl<T extends FullTextSearchableItem> implements FullTextSearchRepository<T> {

    private static final Logger log = Logger.setup(FullTextSearchRepositoryImpl.class);

    private final String cacheName;

    /**
     * the key of the Trie is a single word
     * the key of the inner ConcurrentMap is the item identifier
     */
    private final Trie<String, ConcurrentMap<String, ItemWrapper<T>>> trieMaps = new PatriciaTrie<>();

    // only as a helper map to track by id what we have stored here ...
    private final ConcurrentMap<String, T> itemsByIdHelperMap = new ConcurrentHashMap<>();

    private static final Pattern SPACE_PATTERN = Pattern.compile("\\s");
    private static final Comparator<String> STRING_COMPARATOR = String::compareTo;
    private static final Comparator<String> STRING_LENGTH_DESC_COMPARATOR = Comparator.comparingInt(String::length).reversed();

    private final UpdateHelper updateHelper = new UpdateHelper();
    private final QueryHelper queryHelper = new QueryHelper();
    private final StringHelper stringHelper = new StringHelper();

    private final ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock(false); // false - important for performance reasons
    private final ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = reentrantReadWriteLock.writeLock();

    @Nullable
    private final SynonymsDictionary synonymsDictionary;

    @Nullable
    private final FullTextSearchableItemFactory<T> fullTextSearchableItemFactory;


    /**
     * @param cacheName used for logging purposes
     */
    public FullTextSearchRepositoryImpl(String cacheName) {
        this(cacheName, null, null);
    }

    /**
     * @param cacheName used for logging purposes
     * @param synonymsDictionary dictionary containing synonyms that will be used for items added to this repository
     * @param fullTextSearchableItemFactory needed to create a new instance of {@link FullTextSearchableItem} when synonyms are found for it
     */
    public FullTextSearchRepositoryImpl(String cacheName,
                                        @Nullable SynonymsDictionary synonymsDictionary,
                                        @Nullable FullTextSearchableItemFactory<T> fullTextSearchableItemFactory) {
        this.cacheName = cacheName;
        this.synonymsDictionary = synonymsDictionary;
        this.fullTextSearchableItemFactory = fullTextSearchableItemFactory;
    }

    /**
     * @param item item to be added
     */
    @Override
    public void addItem(T item) {
        LockingWrapper.lockAndWrite(writeLock, updateHelper::addItem, item, "Error adding item to {}; item: {}", cacheName, item);
    }

    @Override
    public void removeItem(T item) {
        LockingWrapper.lockAndWrite(writeLock, updateHelper::removeItem, item, "Error removing item from {}; item: {}", cacheName, item);
    }

    @Override
    public void removeItemById(String itemId) {
        LockingWrapper.lockAndWrite(writeLock, updateHelper::removeItemById, itemId, "Error removing item from {}; itemId: {}", cacheName, itemId);
    }

    @Override
    public List<T> findMatchingItems(String searchText, int maxReturnedItemsLimit, Predicate<FullTextSearchableItem> filter) {
        return LockingWrapper.lockAndGetList(readLock, () -> queryHelper.findMatchingItems(searchText, maxReturnedItemsLimit, filter, null), "Error finding matching items for searchText '{}' in {}!", searchText, cacheName);
    }

    @Override
    public List<T> findMatchingItems(String searchText, int maxReturnedItemsLimit, Predicate<FullTextSearchableItem> filter, Comparator<T> resultSorting) {
        return LockingWrapper.lockAndGetList(readLock, () -> queryHelper.findMatchingItems(searchText, maxReturnedItemsLimit, filter, resultSorting), "Error finding matching items for searchText '{}' in {}!", searchText, cacheName);
    }

    private class UpdateHelper {

        public void addItem(T item) {
            if (!dataCheckOk(item)) {
                return;
            } else {
                addToMaps(item);
            }
        }

        private void addToMaps(T item) {

            item = getItemWithSynonyms(item);

            removePreviousVersionFromTrieMaps(item);

            itemsByIdHelperMap.put(item.getIdentifier(), item);

            List<String> upperCaseNameKeys = stringHelper.sanitizeAndUpper(item.getSearchableNames());
            ItemWrapper<T> itemWrapper = new ItemWrapper<>(item, upperCaseNameKeys);
            List<String> itemWordKeys = itemWrapper.getItemWords();

            for (String word : itemWordKeys) {
                ConcurrentMap<String, ItemWrapper<T>> matchingItems = trieMaps.computeIfAbsent(word, word0 -> new ConcurrentHashMap<>());
                try {
                    matchingItems.put(itemWrapper.getIdentifier(), itemWrapper);
                } catch (Exception e) {
                    log.error("Error putting item id {} to {}", itemWrapper.getIdentifier(), cacheName);
                }
            }
        }

        private void removePreviousVersionFromTrieMaps(T item) {
            boolean alreadyExists = itemsByIdHelperMap.containsKey(item.getIdentifier());
            if (alreadyExists) {
                removeItemById(item.getIdentifier());
            }
        }

        // for debugging ...
        private void logSize(Map<String, ConcurrentMap<Integer, ItemWrapper<T>>> backingTrie) {
            int size = backingTrie.size();
            if (size % 100 == 0) {
                log.debug("Size of {} backingTrie reached {}", cacheName, size);
            }
        }

        public void removeItem(T item) {
            if (!dataCheckOk(item)) {
                return;
            }

            item = getItemWithSynonyms(item);

            List<String> upperCaseNameKeys = stringHelper.sanitizeAndUpper(item.getSearchableNames());

            ItemWrapper<T> itemWrapper = new ItemWrapper<>(item, upperCaseNameKeys);
            for (String itemWordKey : itemWrapper.getItemWords()) {
                // it seems ok to remove items from the trie via this returned internal view
                SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> allPrefixMatchingItemsView = trieMaps.prefixMap(itemWordKey);
                ConcurrentMap<String, ItemWrapper<T>> singleKeyItemsMap = allPrefixMatchingItemsView.get(itemWordKey);
                if (singleKeyItemsMap != null) {
                    singleKeyItemsMap.remove(item.getIdentifier()); // java.lang.NullPointerException: null
                    if (singleKeyItemsMap.isEmpty()) {
                        trieMaps.remove(itemWordKey);
                    } else {
                        // do not remove, other items still mapped to this prefix word
                    }
                }
            }
        }

        private T getItemWithSynonyms(T item) {
            try {
                if (synonymsDictionary != null && fullTextSearchableItemFactory != null) {
                    final List<SynonymsEntry> entries = synonymsDictionary.getEntriesBySearchableItem(item);
                    if (!entries.isEmpty()) {
                        final List<String> searchableNamesWithSynonyms = Stream.concat(item.getSearchableNames().stream(), entries.stream().flatMap(e -> e.synonyms().stream())).distinct().collect(Collectors.toList());
                        return fullTextSearchableItemFactory.from(item.getIdentifier(), searchableNamesWithSynonyms);
                    }
                }
            } catch (Exception e) {
                log.error("Error getting synonyms for item {}", item, e);
            }
            return item;
        }

        public void removeItemById(String itemId) {
            if (itemId != null) {
                T itemToRemove = itemsByIdHelperMap.get(itemId);
                if (itemToRemove != null) {
                    removeItem(itemToRemove);
                }
            }
        }

        private boolean dataCheckOk(T item) {
            if (item == null) {
                log.warn("{} Cannot process null item;", cacheName);
                return false;
            }
            if (item.getIdentifier() == null) {
                log.warn("{} Cannot process item with null id; item: {}", cacheName, item);
                return false;
            }
            if (item.getSearchableNames().isEmpty()) {
                log.warn("{} Cannot process item with empty names; item: {}", cacheName, item);
                return false;
            }
            return true;
        }

    }


    private class QueryHelper {

        public List<T> findMatchingItems(String searchText, int returnedItemsLimit,
                                         Predicate<FullTextSearchableItem> predicate, Comparator<T> resultSorting) {
            if (!isValid(searchText)) {
                return Collections.emptyList();
            } else {
                String searchTextSanitized = stringHelper.sanitizeAndUpper(searchText);
                return search(searchText, searchTextSanitized, returnedItemsLimit, predicate, resultSorting);
            }
        }

        private List<T> search(String searchText, String searchTextSanitized, int returnedItemsLimit,
                               Predicate<FullTextSearchableItem> predicate, Comparator<T> resultSorting) {
            boolean hasMultipleWords = stringHelper.containsSpaces(searchTextSanitized);
            if (hasMultipleWords) {
                return searchByMultipleWordInput(searchTextSanitized, returnedItemsLimit, predicate, resultSorting);
            } else {
                return searchBySingleWordInput(searchText, searchTextSanitized, returnedItemsLimit, predicate, resultSorting);
            }
        }

        private List<T> searchByMultipleWordInput(String searchTextSanitized, int returnedItemsLimit,
                                                  Predicate<FullTextSearchableItem> predicate, Comparator<T> resultSorting) {
            List<String> searchTextWords = stringHelper.splitToMutableList(searchTextSanitized);
            if (!isValid(searchTextWords)) {
                return Collections.emptyList();
            } else {
                SmallestMatch<T> smallestMatch = findSmallestMatch(searchTextWords);
                if (smallestMatch.smallestMatchingMap != null) {
                    return getResultForMultiWordSearch(returnedItemsLimit, searchTextWords, smallestMatch.smallestMatchingMapWord, smallestMatch.smallestMatchingMap, predicate, resultSorting);
                } else {
                    log.debug("{} No items found!", cacheName);
                    return Collections.emptyList();
                }
            }
        }

        private SmallestMatch<T> findSmallestMatch(List<String> searchTextWords) {
            searchTextWords.sort(STRING_LENGTH_DESC_COMPARATOR); // big words first as they might get fewer matches than very short words ... good for faster search
            int smallestMatchingMapSize = -1;
            String smallestMatchingMapWord = null;
            SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap = null;

            for (String searchTextWord : searchTextWords) {
                SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> currPrefixMap = trieMaps.prefixMap(searchTextWord);
                int currMapSize = currPrefixMap.size();
                if (currMapSize == 0) {
                    break; // one of input words does not match -> return nothing ... user needs to correct input
                }
                if (smallestMatchingMapSize > -1) {
                    if (currMapSize < smallestMatchingMapSize) {
                        smallestMatchingMapSize = currMapSize;
                        smallestMatchingMap = currPrefixMap;
                        smallestMatchingMapWord = searchTextWord;
                    }
                } else {
                    smallestMatchingMapSize = currMapSize;
                    smallestMatchingMap = currPrefixMap;
                    smallestMatchingMapWord = searchTextWord;
                }
                if (smallestMatchingMapSize < 5) {
                    // performance shortcut - if current result is small enough we need not go on, this match is good enough ...
                    break;
                }
            }

            return new SmallestMatch<>(smallestMatchingMapWord, smallestMatchingMap);
        }

        private List<T> getResultForMultiWordSearch(int returnedItemsLimit,
                                                    List<String> searchTextWords,
                                                    String smallestMatchingMapWord,
                                                    SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap,
                                                    Predicate<FullTextSearchableItem> predicate,
                                                    Comparator<T> resultSorting) {
            searchTextWords.remove(smallestMatchingMapWord); // this one is already matched as it provided the prefixMap > remove from list
            searchTextWords.sort(STRING_COMPARATOR); // IMPORTANT to be sorted for the rest of the search algorithm to work correctly and to perform best

            Stream<T> stream = smallestMatchingMap.values().stream()
                    .flatMap(stagesMap -> stagesMap.values().stream())
                    .distinct()
                    .filter(itemWrapper -> predicate.test(itemWrapper.item))
                    .filter(itemWrapper -> itemWrapper.matchesAll(searchTextWords))
                    .map(ItemWrapper::getItem);

            if (resultSorting != null) {
                stream = stream.sorted(resultSorting);
            }

            return stream.limit(returnedItemsLimit)
                    .collect(Collectors.toList());
        }

        private List<T> searchBySingleWordInput(String searchText, String searchTextSanitized, int maxReturnedItemsLimit,
                                                Predicate<FullTextSearchableItem> predicate, Comparator<T> resultSorting) {
            SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> prefixMap = trieMaps.prefixMap(searchTextSanitized);
            return getResultForSingleWordSearch(searchText, maxReturnedItemsLimit, prefixMap, predicate, resultSorting);
        }

        private List<T> getResultForSingleWordSearch(String searchText,
                                                     int returnedItemsLimit,
                                                     SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> prefixMap,
                                                     Predicate<FullTextSearchableItem> predicate,
                                                     Comparator<T> resultSorting) {
            logPrefixMapInfo(searchText, prefixMap);

            Stream<T> stream = prefixMap.values().stream()
                    .flatMap(stagesMap -> stagesMap.values().stream())
                    .distinct()
                    .filter(itemWrapper -> predicate.test(itemWrapper.item))
                    .map(ItemWrapper::getItem);

            if (resultSorting != null) {
                stream = stream.sorted(resultSorting);
            }

            return stream.limit(returnedItemsLimit)
                    .collect(Collectors.toList());
        }

        private void logPrefixMapInfo(String searchTextWord, SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> currPrefixMap) {
            log.trace("PrefixMap keys size = {}; total values = {} for key {}", currPrefixMap.keySet().size(), currPrefixMap.values().stream().mapToLong(m -> m.values().size()).sum(), searchTextWord);
        }

        private boolean isValid(String searchText) {
            return searchText != null;
        }

        private boolean isValid(List<String> searchTextWords) {
            return !searchTextWords.isEmpty() && searchTextWords.size() < 15;
        }

    }

    private static class SmallestMatch<T extends FullTextSearchableItem> {
        private final String smallestMatchingMapWord;
        private final SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap;

        public SmallestMatch(String smallestMatchingMapWord,
                             SortedMap<String, ConcurrentMap<String, ItemWrapper<T>>> smallestMatchingMap) {
            this.smallestMatchingMapWord = smallestMatchingMapWord;
            this.smallestMatchingMap = smallestMatchingMap;
        }
    }


    private static class StringHelper {

        private boolean containsSpaces(String searchTextSanitized) {
            return SPACE_PATTERN.matcher(searchTextSanitized).find();
        }

        private String sanitizeAndUpper(String keyBase) {
            return StringUtils.trim(keyBase).toUpperCase();
        }

        private List<String> sanitizeAndUpper(Collection<String> keyBases) {
            return keyBases.stream()
                    .filter(Objects::nonNull)
                    .map(keyBase -> keyBase.trim().toUpperCase())
                    .collect(Collectors.toList());
        }

        private List<String> splitToMutableList(String searchTextSanitized) {
            String[] words = SPACE_PATTERN.split(searchTextSanitized);
            // we need a mutable list so we do not want to use Arrays.asList()
            if (words.length > 0) {
                List<String> wordsList = new ArrayList<>(words.length);
                wordsList.addAll(Arrays.asList(words));
                return wordsList;
            } else {
                return Collections.emptyList();
            }
        }
    }


    /**
     * Encapsulates item data in a format suited for better lookup performance
     */
    private static class ItemWrapper<T extends FullTextSearchableItem> implements FullTextSearchableItem {

        // upper case name of the stored item split into words
        private final T item;
        private final List<String> itemWords;
        private final Collection<String> searchableNames;

        public ItemWrapper(T item, List<String> upperCaseNames) {
            this.item = item;
            this.searchableNames = item.getSearchableNames();
            this.itemWords = upperCaseNames.stream()
                    .flatMap(upperCaseName -> Arrays.stream(SPACE_PATTERN.split(upperCaseName)))
                    .distinct()
                    .sorted(STRING_COMPARATOR) // IMPORTANT! needs to be sorted for better search performance; the search algorithm relies on the being sorted!
                    .map(word -> word.intern()) // intern all words as there are many repetitions // TODO make interning optional
                    .collect(Collectors.toList());
        }

        boolean matchesAll(List<String> searchTextWordsSortedByNaturalOrder) {
            for (String searchTextWord : searchTextWordsSortedByNaturalOrder) {
                boolean matches = false;
                for (int idx = 0; idx < itemWords.size(); idx++) {
                    String itemWord = itemWords.get(idx);
                    if (itemWord.length() >= searchTextWord.length()) {
                        int charDiff = itemWord.charAt(0) - searchTextWord.charAt(0);
                        if (charDiff > 0) {
                            // no following itemWords can match this searchTextWordsSortedByNaturalOrder - can skip the rest
                            return false;
                        } else if (charDiff == 0) {
                            matches = itemWord.startsWith(searchTextWord);
                            if (matches) {
                                break;
                            }
                        } else {
                            // continue
                        }
                    }
                }
                if (!matches) { // if any one search word does not match anything from this item -> quit
                    return false;
                }
            }
            return true; // all passed
        }

        public List<String> getItemWords() {
            return Collections.unmodifiableList(itemWords);
        }

        public T getItem() {
            return item;
        }

        @Override
        public Collection<String> getSearchableNames() {
            return this.searchableNames;
        }

        @Override
        public String getIdentifier() {
            return item.getIdentifier();
        }

        @Override
        public String toString() {
            return "ItemWrapper{" +
                    "item=" + item +
                    ", itemWords=" + itemWords +
                    ", searchableNames=" + searchableNames +
                    '}';
        }
    }

}
