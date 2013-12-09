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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

/**
 * Immutable Binary Search implementation of the Set interface
 *
 * @author tvaliulin
 *
 * @param <E>
 */
public class BinarySearchArraySet<E> extends AbstractArraySet<E> {

    protected Object[] elements;

    public BinarySearchArraySet() {
        super();
    }

    public BinarySearchArraySet(Collection<E> from) {
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
        int index = Arrays.binarySearch(elements, o, comparator());
        if (index < 0) {
            return false;
        }
        // going upward
        for (int i = index; i >= 0 && hashCode(elements[i]) == hash; i--) {
            if (Utils.equal(o, elements[i])) {
                return true;
            }
        }
        // going downward
        for (int i = index + 1; i < size() && hashCode(elements[i]) == hash; i++) {
            if (Utils.equal(o, elements[i])) {
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
        Arrays.sort(elements, comparator());
        return this;
    }
}
