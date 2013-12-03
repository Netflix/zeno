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

import com.netflix.zeno.util.collections.builder.SetBuilder;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Abstract class which helps people to write Array based immutable
 * implementations of the Set interface
 *
 * @author tvaliulin
 *
 * @param <E>
 */
public abstract class AbstractArraySet<E> extends AbstractSet<E> implements SetBuilder<E> {

    public AbstractArraySet() {
    }

    public AbstractArraySet(Collection<E> from) {
        setElements(from);
    }

    protected void setElements(Collection<E> from) {
        builderInit(from.size());
        int i = 0;
        for (E element : from) {
            builderSet(i++, element);
        }
        builderFinish();
    }

    @Override
    public abstract int size();

    @Override
    public abstract boolean contains(Object o);

    @Override
    public Iterator<E> iterator() {
        return new SetIterator();
    }

    protected void removeElement(int index) {
        throw new UnsupportedOperationException();
    }

    protected abstract E element(int index);

    private class SetIterator implements Iterator<E> {
        int cursor;      // index of next element to return
        int lastRet = -1; // index of last element returned; -1 if no such

        @Override
        public boolean hasNext() {
            return cursor != AbstractArraySet.this.size();
        }

        @Override
        public E next() {
            int i = cursor;
            if (i >= AbstractArraySet.this.size())
                throw new NoSuchElementException();
            cursor = i + 1;
            lastRet = i;

            return (E) AbstractArraySet.this.element(lastRet);
        }

        @Override
        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();

            try {
                AbstractArraySet.this.removeElement(lastRet);
                cursor = lastRet;
                lastRet = -1;
            } catch (IndexOutOfBoundsException ex) {
                throw new ConcurrentModificationException();
            }
        }
    }

    protected int hashCode(Object o) {
        return o == null ? 0 : rehash(o.hashCode());
    }

    protected int rehash(int hash) {
        return hash;
    }
}
