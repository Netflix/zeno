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
import com.netflix.zeno.util.collections.algorithms.ArrayQuickSort;
import com.netflix.zeno.util.collections.algorithms.BinarySearch;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Implementation of the Binary Search map with the natural comparator. This
 * implementation requires keys to be comparable.
 *
 * @author tvaliulin
 *
 * @param <K>
 * @param <V>
 */
public class BinarySearchArrayMap<K, V> extends AbstractArraySortedMap<K, V> {
    protected Object[] keysAndValues;

    public BinarySearchArrayMap() {
        setMap(Collections.<K, V> emptyMap());
    }

    public BinarySearchArrayMap(Map<K, V> map) {
        setMap(map);
    }

    public BinarySearchArrayMap(Map.Entry<K, V>[] entries) {
        setMap(entries);
    }

    private BinarySearchArrayMap(AbstractArrayMap<K, V> map, int start, int end) {
        super(map, start, end);
    }

    @Override
    public AbstractArraySortedMap<K, V> newMap(int start, int end) {
        return new BinarySearchArrayMap<K, V>(this, start, end);
    }

    @Override
    public void builderInit(int size) {
        keysAndValues = new Object[size * 2];
    }

    @Override
    public void builderPut(int i, K key, V value) {
        keysAndValues[i * 2] = key;
        keysAndValues[i * 2 + 1] = value;
    }

    @Override
    public SortedMap<K, V> builderFinish() {
        ArrayQuickSort.sort(this, comparator());
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

    @SuppressWarnings("unchecked")
    @Override
    protected K key(int index) {
        int realIndex = index * 2;
        if (realIndex < 0 || realIndex >= keysAndValues.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return (K) keysAndValues[realIndex];
    }

    @SuppressWarnings("unchecked")
    @Override
    protected V value(int index) {
        int realIndex = index * 2 + 1;
        if (realIndex < 0 || realIndex >= keysAndValues.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        return (V) keysAndValues[realIndex];
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object getUndefined(Object key) {

        int index = BinarySearch.binarySearch(this, (K) key, comparator());
        if (index < 0) {
            return AbstractArrayMap.undefined;
        }
        // going upward
        for (int i = index; i >= 0 && (comparator().compare(key(i), (K) key) == 0); i--) {
            if (Utils.equal(key, key(i))) {
                return value(i);
            }
        }
        // going downward
        for (int i = index + 1; i < size() && (comparator().compare(key(i), (K) key) == 0); i++) {
            if (Utils.equal(key, key(i))) {
                return value(i);
            }
        }
        return AbstractArrayMap.undefined;
    }

    @Override
    public Comparator<K> comparator() {
        return Comparators.comparableComparator();
    }
}
