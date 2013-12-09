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
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

/**
 * Immutable Binary Search implementation of the Set interface with hashCode
 * index on the side
 *
 * @author tvaliulin
 *
 * @param <E>
 */
public class BinarySearchArrayIndexedSet<E> extends AbstractArraySet<E> implements Sortable<Integer> {

    protected int[]          hashes;
    protected Object[] elements;

    public BinarySearchArrayIndexedSet() {
        super();
    }

    public BinarySearchArrayIndexedSet(Collection<E> from) {
        super(from);
    }

    @Override
    public int size() {
        return elements.length;
    }

    public Comparator<Object> comparator() {
        return Comparators.hashCodeComparator();
    }

    @Override
    public boolean contains(Object o) {
        int hash = hashCode(o);
        int index = Arrays.binarySearch(hashes, hash);
        if (index < 0) {
            return false;
        }
        // going upward
        for (int i = index; i >= 0 && hashes[i] == hash; i--) {
            if (Utils.equal(o, element(i))) {
                return true;
            }
        }
        // going downward
        for (int i = index + 1; i < size() && hashes[i] == hash; i++) {
            if (Utils.equal(o, element(i))) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected E element(int index) {
        return (E) elements[index];
    }

    @Override
    public void builderInit(int size) {
        elements = new Object[size];
    }

    @Override
    public void builderSet(int index, E element) {
        elements[index] = element;
    }

    @Override
    public Set<E> builderFinish() {
        hashes = new int[elements.length];
        for (int i = 0; i < elements.length; i++) {
            hashes[i] = hashCode(elements[i]);
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

        Object eX = elements[x];
        elements[x] = elements[y];
        elements[y] = eX;

    }
}
