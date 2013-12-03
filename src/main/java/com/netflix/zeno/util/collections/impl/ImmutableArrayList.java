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

import com.netflix.zeno.util.collections.builder.ListBuilder;

import java.util.AbstractList;
import java.util.Collection;
import java.util.List;

/**
 * Immutable implementation of the List interface
 *
 * @author tvaliulin
 *
 * @param <E>
 */
public class ImmutableArrayList<E> extends AbstractList<E> implements ListBuilder<E> {

    protected Object[] elements;

    public ImmutableArrayList() {
    }

    public ImmutableArrayList(Collection<E> collection) {
        setElements(collection);
    }

    protected void setElements(Collection<E> collection) {
        builderInit(collection.size());
        int i = 0;
        for (E entry : collection) {
            builderSet(i++, entry);
        }
        builderFinish();
    }

    @SuppressWarnings("unchecked")
    @Override
    public E get(int index) {
        return (E) elements[index];
    }

    @Override
    public int size() {
        return elements.length;
    }

    @Override
    public void builderInit(int size) {
        this.elements = new Object[size];
    }

    @Override
    public void builderSet(int index, E element) {
        elements[index] = element;
    }

    @Override
    public List<E> builderFinish() {
        return this;
    }
}