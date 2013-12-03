/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.zeno.util.collections;

import com.netflix.zeno.util.collections.builder.Builders;
import com.netflix.zeno.util.collections.builder.ListBuilder;
import com.netflix.zeno.util.collections.builder.MapBuilder;
import com.netflix.zeno.util.collections.builder.SetBuilder;
import com.netflix.zeno.util.collections.impl.BinarySearchArrayHashMap;
import com.netflix.zeno.util.collections.impl.BinarySearchArrayIndexedHashMap;
import com.netflix.zeno.util.collections.impl.BinarySearchArrayIndexedSet;
import com.netflix.zeno.util.collections.impl.BinarySearchArrayMap;
import com.netflix.zeno.util.collections.impl.BinarySearchArraySet;
import com.netflix.zeno.util.collections.impl.ImmutableArrayList;
import com.netflix.zeno.util.collections.impl.NetflixCollections;
import com.netflix.zeno.util.collections.impl.OpenAddressingArraySet;
import com.netflix.zeno.util.collections.impl.OpenAddressingHashMap;
import com.netflix.zeno.util.collections.impl.OpenAddressingSortedHashMap;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class MinimizedUnmodifiableCollections {

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static final SortedMap EMPTY_SORTED_MAP = Collections.unmodifiableSortedMap(new TreeMap());

    private final CollectionImplementation implementation;

    public MinimizedUnmodifiableCollections(CollectionImplementation implementation) {
        this.implementation = implementation;
    }

    @SuppressWarnings("unchecked")
    public <K, V> SortedMap<K, V> emptySortedMap() {
        return (SortedMap<K, V>) EMPTY_SORTED_MAP;
    }

    public <K, V> Map<K, V> minimizeMap(Map<K, V> map) {
        if (map.isEmpty()) {
            return Collections.emptyMap();
        }
        if (map.size() == 1) {
            Map.Entry<K, V> entry = map.entrySet().iterator().next();
            return Collections.singletonMap(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public <K, V> SortedMap<K, V> minimizeSortedMap(SortedMap<K, V> map) {
        if (map.isEmpty()) {
            return NetflixCollections.emptySortedMap();
        }
        if (map.size() == 1) {
            Map.Entry<K, V> entry = map.entrySet().iterator().next();
            return NetflixCollections.singletonSortedMap(entry.getKey(), entry.getValue());
        }
        return map;
    }

    public <E> Set<E> minimizeSet(Set<E> set) {
        if (set.isEmpty()) {
            return Collections.emptySet();
        }
        if (set.size() == 1) {
            return Collections.singleton(set.iterator().next());
        }
        return set;
    }

    public <E> List<E> minimizeList(List<E> list) {
        if (list.isEmpty()) {
            return Collections.emptyList();
        }
        if (list.size() == 1) {
            return Collections.singletonList(list.iterator().next());
        }
        return list;
    }

    public <E> ListBuilder<E> createListBuilder() {
        switch (implementation) {
            case COMPACT_BINARYSEARCH:
            case COMPACT_BINARYSEARCH_INDEXED:
            case COMPACT_OPENADDRESS:
                return new ImmutableArrayList<E>();
            case JAVA_UTIL:
            default:
                return new Builders.ArrayListBuilder<E>();
        }
    }

    public <E> SetBuilder<E> createSetBuilder() {
        switch (implementation) {
            case COMPACT_BINARYSEARCH:
                return new BinarySearchArraySet<E>();
            case COMPACT_BINARYSEARCH_INDEXED:
                return new BinarySearchArrayIndexedSet<E>();
            case COMPACT_OPENADDRESS:
                return new OpenAddressingArraySet<E>();
            case JAVA_UTIL:
            default:
                return new Builders.HashSetBuilder<E>();
        }
    }

    public <K, V> MapBuilder<K, V> createMapBuilder() {
        switch (implementation) {
            case COMPACT_BINARYSEARCH:
                return new BinarySearchArrayHashMap<K, V>();
            case COMPACT_BINARYSEARCH_INDEXED:
                return new BinarySearchArrayIndexedHashMap<K, V>();
            case COMPACT_OPENADDRESS:
                return new OpenAddressingHashMap<K, V>();
            case JAVA_UTIL:
            default:
                return new Builders.HashMapBuilder<K, V>();
        }
    }

    public <K, V> MapBuilder<K, V> createSortedMapBuilder() {
        switch (implementation) {
            case COMPACT_BINARYSEARCH:
            case COMPACT_BINARYSEARCH_INDEXED:
                return new BinarySearchArrayMap<K, V>();
            case COMPACT_OPENADDRESS:
                return new OpenAddressingSortedHashMap<K, V>();
            case JAVA_UTIL:
            default:
                return new Builders.TreeMapBuilder<K, V>();
        }
    }

}
