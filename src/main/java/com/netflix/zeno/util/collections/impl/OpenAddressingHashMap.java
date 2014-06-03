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

import com.netflix.zeno.util.collections.builder.MapBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Open Addressing hash map immutable implementation of the Map interface
 *
 * @author dkoszevnik
 * @author tvaliulin
 *
 * @param <K>
 * @param <V>
 */
public class OpenAddressingHashMap<K, V> extends AbstractArrayMap<K, V> implements MapBuilder<K, V> {

    // hashTable will be byte[], short[], or int[], depending on how many entries the Map has.
    // hashTable[i] points to the index of a key in the entries[] array which
    // hashes to i.
    protected Object hashTable;

    // entries.length is 2*number of key/value pairs. All keys are located at
    // even array indices.
    // the value for a given key is located at entries[keyIndex + 1];
    protected Object keysAndValues[];

    protected int size;

    public OpenAddressingHashMap() {
        setMap(Collections.<K, V> emptyMap());
    }

    public OpenAddressingHashMap(Map<K, V> map) {
        setMap(map);
    }

    public OpenAddressingHashMap(Map.Entry<K, V>[] entries) {
        setMap(entries);
    }

    public OpenAddressingHashMap(AbstractArrayMap<K, V> map, int start, int end) {
        super(map, start, end);
    }

    // 70% load factor
    public float loadFactor() {
        return 0.7f;
    }

    @Override
    public void builderInit(int numEntries) {
        hashTable = OpenAddressing.newHashTable(numEntries, loadFactor());
        keysAndValues = new Object[numEntries * 2];
    }

    @Override
    public void builderPut(int index, K key, V value) {
        keysAndValues[size * 2] = key;
        keysAndValues[(size * 2) + 1] = value;
        size++;
    }

    @Override
    public Map<K, V> builderFinish() {
        // / Math.abs(x % n) is the same as (x & n-1) when n is a power of 2
        int hashModMask = OpenAddressing.hashTableLength(hashTable) - 1;

        if(keysAndValues.length > size * 2)
            keysAndValues = Arrays.copyOf(keysAndValues, size * 2);

        for (int i = 0; i < keysAndValues.length; i += 2) {
            int hash = hashCode(keysAndValues[i]);
            int bucket = hash & hashModMask;

            /// linear probing resolves collisions
            while (OpenAddressing.getHashEntry(hashTable, bucket) != -1) {
                bucket = (bucket + 1) & hashModMask;
            }

            OpenAddressing.setHashEntry(hashTable, bucket, i / 2);
        }
        return this;
    }

    @Override
    protected int rehash(int hash) {
        return OpenAddressing.rehash(hash);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected K key(int index) {
        return (K) keysAndValues[index * 2];
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V value(int index) {
        return (V) keysAndValues[index * 2 + 1];
    }

    /**
     * If finish() has already been called on this map, this method returns the
     * value associated with the specified key. If the specified key is not in
     * this map, returns null.
     *
     * If finish() has not been called on this map, this method always returns
     * null.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Object getUndefined(Object key) {
        // Math.abs(x % n) is the same as (x & n-1) when n is a power of 2
        int hashModMask = OpenAddressing.hashTableLength(hashTable) - 1;
        int hash = hashCode(key);
        int bucket = hash & hashModMask;

        int hashEntry = OpenAddressing.getHashEntry(hashTable, bucket) * 2;

        // We found an entry at this hash position
        while (hashEntry >= 0) {
            if (Utils.equal(keysAndValues[hashEntry], key)) {
                return (V) keysAndValues[hashEntry + 1];
            }

            // linear probing resolves collisions.
            bucket = (bucket + 1) & hashModMask;
            hashEntry = OpenAddressing.getHashEntry(hashTable, bucket) * 2;
        }
        return AbstractArrayMap.undefined;
    }
}
