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
import com.netflix.zeno.util.collections.algorithms.BinarySearch;
import com.netflix.zeno.util.collections.builder.MapBuilder;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Abstract class which helps people to write Array based implementations of the
 * immutable SortedMap interface
 *
 * @author tvaliulin
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractArraySortedMap<K, V> extends AbstractArrayMap<K, V> implements SortedMap<K, V>, Sortable<K>, Comparable<AbstractArrayMap<K, V>>, MapBuilder<K, V> {

    public AbstractArraySortedMap() {
    }

    public AbstractArraySortedMap(AbstractArrayMap<K, V> map, int start, int end) {
        super(map, start, end);
    }

    @Override
    public abstract Comparator<K> comparator();

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

    public abstract SortedMap<K, V> newMap(int start, int end);

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

    @Override
    public K at(int index) {
        return key(index);
    }

    @Override
    public abstract void swap(int x, int y);

    @Override
    public int compareTo(AbstractArrayMap<K, V> o) {
        if (o == this)
            return 0;

        if (!(o instanceof BinarySearchArrayMap)) {
            return getClass().getCanonicalName().compareTo(o.getClass().getCanonicalName());
        }
        Map<K, V> m = (Map<K, V>) o;

        if (size() == 0 && m.size() == 0) {
            return 0;
        }
        Iterator<Map.Entry<K, V>> itSelf = entrySet().iterator();
        Iterator<Map.Entry<K, V>> itOther = m.entrySet().iterator();

        for (;;) {
            boolean selfNext = itSelf.hasNext();
            boolean otherNext = itOther.hasNext();
            if (!selfNext && !otherNext) {
                return 0;
            }
            if (!selfNext && otherNext) {
                return -1;
            }
            if (selfNext && !otherNext) {
                return 1;
            }
            Map.Entry<K, V> selfEntry = itSelf.next();
            Map.Entry<K, V> otherEntry = itOther.next();
            int keyCompare = comparator().compare(selfEntry.getKey(), otherEntry.getKey());
            if (keyCompare != 0) {
                return keyCompare;
            }
            int valueCompare = Comparators.comparableComparator().compare(selfEntry.getValue(), otherEntry.getValue());
            if (valueCompare != 0) {
                return valueCompare;
            }
        }
    }
}
