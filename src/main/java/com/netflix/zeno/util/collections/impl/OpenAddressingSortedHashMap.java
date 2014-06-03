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
package com.netflix.zeno.util.collections.impl;

import com.netflix.zeno.util.collections.Comparators;
import com.netflix.zeno.util.collections.algorithms.Sortable;
import com.netflix.zeno.util.collections.algorithms.ArrayQuickSort;
import com.netflix.zeno.util.collections.algorithms.BinarySearch;
import com.netflix.zeno.util.collections.builder.MapBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Open Addressing hash map immutable implementation of the SortedMap interface
 *
 * @author tvaliulin
 * @author dkoszevnik
 *
 * @param <K>
 * @param <V>
 */
public class OpenAddressingSortedHashMap<K, V> extends OpenAddressingHashMap<K, V> implements MapBuilder<K, V>, SortedMap<K, V>, Sortable<K> {

    public OpenAddressingSortedHashMap() {
        setMap(Collections.<K, V> emptyMap());
    }

    public OpenAddressingSortedHashMap(Map<K, V> map) {
        setMap(map);
    }

    public OpenAddressingSortedHashMap(AbstractArrayMap<K, V> map, int start, int end) {
        super(map, start, end);
    }

    public OpenAddressingSortedHashMap(Map.Entry<K, V>[] entries) {
        setMap(entries);
    }

    @Override
    public SortedMap<K, V> builderFinish() {
        if(keysAndValues.length > size * 2)
            keysAndValues = Arrays.copyOf(keysAndValues, size * 2);

        if (comparator() != null) {
            ArrayQuickSort.sort(this, comparator());
        }
        super.builderFinish();
        return this;
    }

    @Override
    public K at(int index) {
        return key(index);
    }

    @Override
    public void swap(int x, int y) {
        Utils.Array.swap(keysAndValues, x, y);
    }

    @Override
    public int size() {
        return keysAndValues.length / 2;
    }

    @Override
    public Comparator<K> comparator() {
        return Comparators.comparableComparator();
    }

    public SortedMap<K, V> newMap(int start, int end) {
        return new OpenAddressingSortedHashMap<K, V>(this, start, end);
    }

    /// SortedMap implementation ///
    @SuppressWarnings("unchecked")
    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        int start = 0;
        if (fromKey != null) {
            start = BinarySearch.binarySearch(this, fromKey, (Comparator<Object>) comparator());
            start = ((start >= 0) ? start : (-start - 1));
            for (int i = start; i >= 0 && i < size() && comparator().compare(key(i), fromKey) >= 0; i--) {
                start = i;
            }
        }
        int end = size();
        if (toKey != null) {
            end = BinarySearch.binarySearch(this, toKey, (Comparator<Object>) comparator());
            end = ((end >= 0) ? end : (-end - 1));
            for (int i = end; i >= 0 && i < size() && comparator().compare(key(i), toKey) < 0; i++) {
                end = i;
            }
        }
        start = Math.max(start, 0);
        end = Math.min(end, size());
        return newMap(start, end);
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return subMap(null, toKey);
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return subMap(fromKey, null);
    }

    @Override
    public K firstKey() {
        return key(0);
    }

    @Override
    public K lastKey() {
        return key(size() - 1);
    }
}
