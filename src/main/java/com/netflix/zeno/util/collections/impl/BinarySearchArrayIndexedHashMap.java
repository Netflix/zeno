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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Implementation of the BinarySearch Map with the hashCode search index on a
 * side
 *
 * @author tvaliulin
 *
 * @param <K>
 * @param <V>
 */
public class BinarySearchArrayIndexedHashMap<K, V> extends AbstractArrayMap<K, V> implements Sortable<Integer> {
    protected int[]    hashes        = null;
    protected Object[] keysAndValues = null;

    public BinarySearchArrayIndexedHashMap() {
        setMap(Collections.<K, V> emptyMap());
    }

    public BinarySearchArrayIndexedHashMap(Map<K, V> map) {
        setMap(map);
    }

    public BinarySearchArrayIndexedHashMap(Map.Entry<K, V>[] entries) {
        setMap(entries);
    }

    public BinarySearchArrayIndexedHashMap(AbstractArrayMap<K, V> map, int start, int end) {
        super(map, start, end);
    }

    @Override
    public void builderInit(int size) {
        keysAndValues = new Object[size * 2];
    }

    @Override
    public void builderPut(int index, K key, V value) {
        keysAndValues[index * 2] = key;
        keysAndValues[index * 2 + 1] = value;
    }

    @Override
    public Map<K, V> builderFinish() {
        hashes = new int[keysAndValues.length / 2];
        for (int i = 0; i < keysAndValues.length / 2; i++) {
            hashes[i] = hashCode(keysAndValues[i * 2]);
        }
        ArrayQuickSort.<Integer> sort(this, Comparators.<Integer> comparableComparator());
        return this;
    }

    @Override
    public Integer at(int index) {
        return hashes[index];
    }

    @Override
    public void swap(int x, int y) {
        int hashX = hashes[x];
        hashes[x] = hashes[y];
        hashes[y] = hashX;

        Utils.Array.swap(keysAndValues, x, y);
    }

    @Override
    public int size() {
        return hashes.length;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected K key(int index) {
        return (K) keysAndValues[index * 2];
    }

    @SuppressWarnings("unchecked")
    @Override
    protected V value(int index) {
        return (V) keysAndValues[index * 2 + 1];
    }

    @Override
    public Object getUndefined(Object key) {
        int hash = hashCode(key);
        int index = Arrays.binarySearch(hashes, hash);
        if (index < 0) {
            return AbstractArrayMap.undefined;
        }
        // going upward
        for (int i = index; i >= 0 && hashes[i] == hash; i--) {
            if (Utils.equal(key, key(i))) {
                return value(i);
            }
        }
        // going downward
        for (int i = index + 1; i < size() && hashes[i] == hash; i++) {
            if (Utils.equal(key, key(i))) {
                return value(i);
            }
        }
        return AbstractArrayMap.undefined;
    }
}
