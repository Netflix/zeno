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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 *
 * The AbstractHeapFriendlyHashMap is an open-addressing, linear probing hash table.  There are two implementations,<p/>
 *
 * HeapFriendlyHashMap - which uses two segmented arrays for keys and values<br/>
 * HeapFriendlyDerivableKeyHashMap - which uses a single segmented array for values, and the keys are trivially derivable from the values.<p/>
 *
 * The segmented arrays are composed individual Object[] arrays which are each 4096 elements long.
 *
 * @see HeapFriendlyMapArrayRecycler
 *
 * @author dkoszewnik
 *
 */
public abstract class AbstractHeapFriendlyMap<K, V> extends AbstractMap<K, V> {

    @SuppressWarnings("unchecked")
    protected Object segmentedGet(Object[][] segmentedArray, int bucket) {
        int arrayIndex = bucket / INDIVIDUAL_OBJECT_ARRAY_SIZE;
        int elementIndex = bucket % INDIVIDUAL_OBJECT_ARRAY_SIZE;

        return (V) segmentedArray[arrayIndex][elementIndex];
    }

    protected void segmentedSet(Object[][] segmentedArray, int bucket, Object value) {
        int arrayIndex = bucket / INDIVIDUAL_OBJECT_ARRAY_SIZE;
        int elementIndex = bucket % INDIVIDUAL_OBJECT_ARRAY_SIZE;

        segmentedArray[arrayIndex][elementIndex] = value;
    }

    public abstract void releaseObjectArrays();

    protected void releaseObjectArrays(Object[][] segmentedArray, HeapFriendlyMapArrayRecycler recycler) {

        for(int i=0;i<segmentedArray.length;i++) {
            recycler.returnObjectArray(segmentedArray[i]);
        }
    }


    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("VMS error: Cannot remove items from a HeapFriendlyMap");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("VMS error: HeapFriendlyMap cannot be added to with a specified key.  Please use put(V value).");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("VMS error: Cannot clear a HeapFriendlyMap.");
    }

    protected class HeapFriendlyMapIterator<T> implements Iterator<T> {
        protected final Object[][] segmentedArray;
        protected final int numBuckets;
        protected int current = -1;

        protected HeapFriendlyMapIterator(Object[][] segmentedArray, int numBuckets) {
            this.segmentedArray = segmentedArray;
            this.numBuckets = numBuckets;
            moveToNext();
        }

        public boolean hasNext() {
            return current < numBuckets;
        }

        @SuppressWarnings("unchecked")
        public T next() {
            if(current >= numBuckets)
                throw new NoSuchElementException();

            T val = (T) segmentedGet(segmentedArray, current);
            moveToNext();
            return val;
        }

        protected void moveToNext() {
            current++;
            while(current < numBuckets && segmentedGet(segmentedArray, current) == null) {
                current++;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("VMS error:  Cannot remove from a HeapFriendlyMapIterator");
        }
    }

    protected static abstract class AbstractHeapFriendlyMapEntry<K, V> implements Map.Entry<K, V> {
        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException("Cannot set value for HeapFriendlyMap Entry");
        }

        @Override
        @SuppressWarnings("rawtypes")
        public boolean equals(Object o) {
            if(Entry.class.isAssignableFrom(o.getClass())) {
                Entry other = (Entry)o;

                return (getKey()==null ?
                             other.getKey()==null : getKey().equals(other.getKey()))  &&
                            (getValue()==null ?
                             other.getValue()==null : getValue().equals(other.getValue()));
            }

            return false;
        }

        @Override
        public int hashCode() {
            return (getKey()==null   ? 0 : getKey().hashCode()) ^
                    (getValue()==null ? 0 : getValue().hashCode());
        }

        public String toString() {
            return getKey() + "=" + getValue();
        }
    }


}
