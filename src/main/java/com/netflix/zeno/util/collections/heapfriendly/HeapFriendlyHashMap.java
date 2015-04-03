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
package com.netflix.zeno.util.collections.heapfriendly;

import static com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyMapArrayRecycler.INDIVIDUAL_OBJECT_ARRAY_SIZE;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 *
 * The HeapFriendlyHashMap is an open-addressed, linear probing hash table.  It uses two segmented arrays, one to hold the keys
 * and one to hold the values.
 *
 * @author dkoszewnik
 *
 */
public class HeapFriendlyHashMap<K, V> extends AbstractHeapFriendlyMap<K, V> {

    private final Object[][] keys;
    private final Object[][] values;
    private final int numBuckets;
    private final int maxSize;
    private final HeapFriendlyMapArrayRecycler recycler;
    private int size;

    public HeapFriendlyHashMap(int numEntries) {
        this(numEntries, HeapFriendlyMapArrayRecycler.get());
    }

    public HeapFriendlyHashMap(int numEntries, HeapFriendlyMapArrayRecycler recycler) {
        int arraySize = numEntries * 10 / 7; // 70% load factor
        arraySize = 1 << (32 - Integer.numberOfLeadingZeros(arraySize)); // next power of 2
        arraySize = Math.max(arraySize, INDIVIDUAL_OBJECT_ARRAY_SIZE);

        this.numBuckets = arraySize;
        this.maxSize = numEntries;
        this.recycler = recycler;
        this.keys = createSegmentedObjectArray(arraySize);
        this.values = createSegmentedObjectArray(arraySize);
    }

    private Object[][] createSegmentedObjectArray(int arraySize) {
        int numArrays = arraySize / INDIVIDUAL_OBJECT_ARRAY_SIZE;

        Object[][] segmentedArray = new Object[numArrays][];

        for(int i=0;i<numArrays;i++) {
            segmentedArray[i] = recycler.getObjectArray();
        }

        return segmentedArray;
    }

    @SuppressWarnings("unchecked")
    public V put(K key, V value) {
        if(size >= maxSize && !containsKey(key))
            throw new UnsupportedOperationException("Cannot add more elements than " + maxSize);

        if(key == null || value == null)
            throw new NullPointerException("Null keys / values not supported in HeapFriendlyHashMap");

        int hashCode = rehash(key.hashCode());

        /// numBuckets is a power of 2, so the operation [x & (numBuckets - 1)]
        /// is equivalent to [Math.abs(x % numBuckets)]
        int bucket = hashCode & (numBuckets - 1);
        K foundKey = (K) segmentedGet(keys, bucket);

        while(foundKey != null && !foundKey.equals(key)) {
            bucket = (bucket + 1) & (numBuckets - 1);
            foundKey = (K) segmentedGet(keys, bucket);
        }

        V foundValue = (V) segmentedGet(values, bucket);

        segmentedSet(keys, bucket, key);
        segmentedSet(values, bucket, value);

        if(foundValue == null)
            size++;

        return foundValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        if(key == null)
            return null;

        int hashCode = rehash(key.hashCode());

        /// numBuckets is a power of 2, so the operation [x & (numBuckets - 1)]
        /// is equivalent to [Math.abs(x % numBuckets)]
        int bucket = hashCode & (numBuckets - 1);
        K foundKey = (K) segmentedGet(keys, bucket);

        while(foundKey != null) {
            if(foundKey.equals(key)) {
                return (V) segmentedGet(values, bucket);
            }
            bucket = (bucket + 1) & (numBuckets - 1);
            foundKey = (K) segmentedGet(keys, bucket);
        }

        return null;
    }



    @Override
    public boolean containsKey(Object key) {
        if(key == null)
            return false;

        return get(key) != null;
    }


    private int rehash(int hash) {
        hash = ~hash + (hash << 15);
        hash = hash ^ (hash >>> 12);
        hash = hash + (hash << 2);
        hash = hash ^ (hash >>> 4);
        hash = hash * 2057;
        hash = hash ^ (hash >>> 16);
        return hash;
    }



    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean containsValue(Object value) {
        for(V foundValue : values()) {
            if(foundValue.equals(value)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Set<K> keySet() {
        return new AbstractSet<K>() {
            public Iterator<K> iterator() {
                return new HeapFriendlyMapIterator<K>(keys, numBuckets);
            }

            @Override
            public boolean contains(Object value) {
                return containsKey(value);
            }

            @Override
            public int size() {
                return size;
            }
        };

    }

    @Override
    public Collection<V> values() {
        return new AbstractSet<V>() {
            @Override
            public Iterator<V> iterator() {
                return new HeapFriendlyMapIterator<V>(values, numBuckets);
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<Map.Entry<K, V>>() {
            @Override
            public Iterator<Map.Entry<K, V>> iterator() {
                return new HeapFriendlyMapIterator<Map.Entry<K,V>>(keys, numBuckets) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public Map.Entry<K, V> next() {
                        if(current >= numBuckets)
                            throw new NoSuchElementException();

                        K key = (K) segmentedGet(segmentedArray, current);

                        Entry<K, V> entry = new Entry<K, V>(key, (V) segmentedGet(values, current));
                        moveToNext();
                        return entry;
                    }
                };
            }

            @Override
            public int size() {
                return size;
            }
        };
    }

    @Override
    public void releaseObjectArrays() {
        releaseObjectArrays(keys, recycler);
        releaseObjectArrays(values, recycler);
    }

    private static class Entry<K, V> extends AbstractHeapFriendlyMapEntry<K, V> {
        private final K key;
        private final V value;

        public Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }
    }
}
