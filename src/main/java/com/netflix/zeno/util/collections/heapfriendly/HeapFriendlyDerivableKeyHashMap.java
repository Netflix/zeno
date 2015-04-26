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
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The HeapFriendlyDerivableKeyHashMap is an open-addressed, linear probing hash table.  It uses a single array to hold only the values,
 * and assumes that keys are derivable from values.  The HeapFriendlyDerivableKeyHashMap may be used to conserve memory.<p/>
 *
 * HeapFriendlyDerivableKeyHashMap must be extended to override the deriveKey method.<p/>
 *
 * The key should be derivable from the value without causing any overhead.  For example, if K is a field in V, then
 * the implementation may be as simple as "return value.getKey();".<p/>
 *
 * However, if a compound key is required, or if any non-trivial amount of work must be done to derive the key, the
 * HeapFriendlyDerivableKeyHashMap may not be appropriate.  Instead see {@link HeapFriendlyHashMap}
 *
 * @author dkoszewnik
 *
 */
public abstract class HeapFriendlyDerivableKeyHashMap<K, V> extends AbstractHeapFriendlyMap<K, V> {

    private final Object[][] values;
    private final int numBuckets;
    private final int maxSize;
    private final HeapFriendlyMapArrayRecycler recycler;

    private int size;

    protected HeapFriendlyDerivableKeyHashMap(int numEntries) {
        this(numEntries, HeapFriendlyMapArrayRecycler.get());
    }

    protected HeapFriendlyDerivableKeyHashMap(int numEntries, HeapFriendlyMapArrayRecycler recycler) {
        int arraySize = numEntries * 10 / 7; // 70% load factor
        arraySize = 1 << (32 - Integer.numberOfLeadingZeros(arraySize)); // next power of 2
        arraySize = Math.max(arraySize, INDIVIDUAL_OBJECT_ARRAY_SIZE);

        this.numBuckets = arraySize;
        this.maxSize = numEntries;
        this.recycler = recycler;
        values = createSegmentedObjectArray(arraySize);
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
    public V put(V value) {
        if(size == maxSize && !containsKey(deriveKey(value)))
            throw new UnsupportedOperationException("Cannot add more elements than " + maxSize);

        K key = deriveKey(value);

        if(key == null) {
            throw new NullPointerException("Null keys not allowed in HeapFriendlyDerivableKeyHashMap");
        }

        int hashCode = rehash(key.hashCode());

        /// numBuckets is a power of 2, so the operation [x & (numBuckets - 1)]
        /// is equivalent to [Math.abs(x % numBuckets)]
        int bucket = hashCode & (numBuckets - 1);
        V foundValue = (V) segmentedGet(values, bucket);

        while(foundValue != null && !deriveKey(foundValue).equals(deriveKey(value))) {
            bucket = (bucket + 1) & (numBuckets - 1);
            foundValue = (V) segmentedGet(values, bucket);
        }

        segmentedSet(values, bucket, value);

        if(foundValue == null)
            size++;

        return foundValue;
    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {
        int hashCode = rehash(key.hashCode());

        int bucket = hashCode & (numBuckets - 1);
        V foundValue = (V) segmentedGet(values, bucket);

        while(foundValue != null) {
            if(deriveKey(foundValue).equals(key))
                return foundValue;

            bucket = (bucket + 1) & (numBuckets - 1);
            foundValue = (V) segmentedGet(values, bucket);
        }

        return null;
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
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsValue(Object value) {
        if(value == null)
            return false;

        return get(deriveKey((V)value)) != null;

    }

    @Override
    public void releaseObjectArrays() {
        releaseObjectArrays(values, recycler);
    }

    @Override
    public Set<K> keySet() {
        return new AbstractSet<K>() {
            public Iterator<K> iterator() {
                return new HeapFriendlyMapIterator<K>(values, numBuckets) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public K next() {
                        K key = deriveKey((V) segmentedGet(values, current));
                        moveToNext();
                        return key;
                    }
                };
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
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<Entry<K, V>>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new HeapFriendlyMapIterator<Entry<K,V>>(values, numBuckets) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public Entry<K, V> next() {
                        if(current >= numBuckets)
                            throw new NoSuchElementException();

                        Entry<K, V> entry = new DerivableKeyHashMapEntry((V) segmentedGet(segmentedArray, current));
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

    /**
     * Each implementation of HeapFriendlyDerivableKeyHashMap must be overridden to implement the deriveKey method.
     *
     * The key should be derivable from the value without causing any overhead.  For example, if K is a field in V, then
     * the implementation may be as simple as "return value.getKey();".
     *
     * However, if a compound key is required, or if any non-trivial amount of work must be done to derive the key, the
     * HeapFriendlyDerivableKeyHashMap may not be appropriate.  Instead see {@link HeapFriendlyHashMap}
     *
     */
    protected abstract K deriveKey(V value);

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException("VMS error: HeapFriendlyDerivableKeyMap cannot be added to with a specified key.  Please use put(V value).");
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

    private class DerivableKeyHashMapEntry extends AbstractHeapFriendlyMapEntry<K, V> {
        private final V value;

        DerivableKeyHashMapEntry(V value) {
            this.value = value;
        }

        @Override
        public K getKey() {
            return deriveKey(value);
        }

        @Override
        public V getValue() {
            return value;
        }
    }
}
