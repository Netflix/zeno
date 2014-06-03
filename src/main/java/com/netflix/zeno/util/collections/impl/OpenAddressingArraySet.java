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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

/**
 * Open addressing hash set immutable implementation of the Set interface
 *
 * @author dkoszevnik
 * @author tvaliulin
 *
 * @param <E>
 */
public class OpenAddressingArraySet<E> extends AbstractArraySet<E> {

    private Object hashTable;
    private Object elements[];
    private int size;

    public OpenAddressingArraySet() {
        super();
    }

    public OpenAddressingArraySet(Collection<E> from) {
        super(from);
    }

    @Override
    public int size() {
        return size;
    }

    // 70% load factor
    public float loadFactor() {
        return 0.7f;
    }

    @Override
    public boolean contains(Object o) {
        // / Math.abs(x % n) is the same as (x & n-1) when n is a power of 2
        int hashModMask = OpenAddressing.hashTableLength(hashTable) - 1;
        int hash = hashCode(o);
        int bucket = hash & hashModMask;

        int hashEntry = OpenAddressing.getHashEntry(hashTable, bucket);

        // We found an entry at this hash position
        while (hashEntry >= 0) {
            if (Utils.equal(elements[hashEntry], o)) {
                return true;
            }

            // linear probing resolves collisions.
            bucket = (bucket + 1) & hashModMask;
            hashEntry = OpenAddressing.getHashEntry(hashTable, bucket);
        }
        return false;

    }

    @SuppressWarnings("unchecked")
    @Override
    protected E element(int index) {
        return (E) elements[index];
    }

    @Override
    public void builderInit(int numEntries) {
        hashTable = OpenAddressing.newHashTable(numEntries, loadFactor());
        elements = new Object[numEntries];
    }

    @Override
    public void builderSet(int index, E element) {
        elements[size++] = element;
    }

    @Override
    public Set<E> builderFinish() {
        if(elements.length > size)
            elements = Arrays.copyOf(elements, size);

        // Math.abs(x % n) is the same as (x & n-1) when n is a power of 2
        int hashModMask = OpenAddressing.hashTableLength(hashTable) - 1;

        for (int i = 0; i < elements.length; i++) {
            int hash = hashCode(elements[i]);
            int bucket = hash & hashModMask;

            // / linear probing resolves collisions
            while (OpenAddressing.getHashEntry(hashTable, bucket) != -1) {
                bucket = (bucket + 1) & hashModMask;
            }

            OpenAddressing.setHashEntry(hashTable, bucket, i);
        }
        return this;
    }

    @Override
    protected int rehash(int hash) {
        return OpenAddressing.rehash(hash);
    }
}
