package com.snapscore.pipeline.textsearch;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.junit.Test;

import java.util.Map;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;

public class PatritiaTreeDemo {


    @Test
    public void demo() {

        final Trie<String, String> patriciaTrie = new PatriciaTrie<>();

        final String key1 = "hi there!";
        final String key2 = "hi here!";
        final String key3 = "there!";

        patriciaTrie.put(key1, key1);
        patriciaTrie.put(key2, key2);
        patriciaTrie.put(key3, key3);

        SortedMap<String, String> prefixMap;

        // example 1

        prefixMap = patriciaTrie.prefixMap("hi");

        assertEquals(2, prefixMap.size());
        assertEquals(Map.of(key1, key1, key2, key2), prefixMap);

        // example 2

        prefixMap = patriciaTrie.prefixMap("hi th");

        assertEquals(1, prefixMap.size());
        assertEquals(Map.of(key1, key1), prefixMap);

        // example 3

        prefixMap = patriciaTrie.prefixMap("th");

        assertEquals(1, prefixMap.size());
        assertEquals(Map.of(key3, key3), prefixMap);

        // example 4

        prefixMap = patriciaTrie.prefixMap("here");

        assertEquals(0, prefixMap.size());

    }
}
