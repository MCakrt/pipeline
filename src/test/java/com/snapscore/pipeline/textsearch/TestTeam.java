package com.snapscore.pipeline.textsearch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class TestTeam implements FullTextSearchableItem {

    final String id;
    final List<String> names;
    final boolean placeholder;

    public TestTeam(String id, String name) {
        this.id = id;
        this.names = List.of(name);
        this.placeholder = false;
    }


    public TestTeam(String id, List<String> names) {
        this.id = id;
        this.names = names;
        this.placeholder = false;
    }

    public TestTeam(String id, Collection<String> names) {
        this.id = id;
        this.names = new ArrayList<>(names);
        this.placeholder = false;
    }

    public TestTeam(String id, String name, boolean placeholder) {
        this.id = id;
        this.names = List.of(name);
        this.placeholder = placeholder;
    }

    @Override
    public List<String> getSearchableNames() {
        return names;
    }

    @Override
    public String getIdentifier() {
        return id;
    }
}
