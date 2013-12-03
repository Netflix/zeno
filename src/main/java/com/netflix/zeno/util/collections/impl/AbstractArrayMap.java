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

import java.util.AbstractCollection;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Abstract class which helps people to write Array based immutable
 * implementations of the Map interface
 *
 * @author tvaliulin
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractArrayMap<K, V> implements Map<K, V>, MapBuilder<K, V> {

    protected static final Object undefined = new Object();

    public AbstractArrayMap() {
    }

    public AbstractArrayMap(AbstractArrayMap<K, V> map, int start, int end) {
        builderInit(end - start);
        for (int i = start; i < end; i++) {
            builderPut(i - start, map.key(i), map.value(i));
        }
        builderFinish();
    }

    public abstract Object getUndefined(Object key);

    @Override
    public abstract int size();

    protected abstract K key(int index);

    protected abstract V value(int index);

    @Override
    public abstract void builderInit(int size);

    @Override
    public abstract void builderPut(int index, K key, V value);

    @Override
    public abstract Map<K, V> builderFinish();

    public void setMap(Map<K, V> map) {
        builderInit(map.size());
        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            builderPut(i++, entry.getKey(), entry.getValue());
        }
        builderFinish();
    }

    public void setMap(Map.Entry<K, V>[] entries) {
        builderInit(entries.length);
        int i = 0;
        for (Map.Entry<K, V> entry : entries) {
            builderPut(i++, entry.getKey(), entry.getValue());
        }
        builderFinish();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        Object result = getUndefined(key);
        if (result == undefined) {
            return null;
        } else {
            return (V) result;
        }
    }

    @Override
    public boolean containsKey(Object key) {
        return undefined != getUndefined(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (int i = 0; i < size(); i++) {

            if (value(i) == value) {
                return true;
            }
            if (value != null && value.equals(value(i))) {
                return true;
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof Map))
            return false;
        Map<K, V> m = (Map<K, V>) o;
        if (m.size() != size())
            return false;

        try {
            Iterator<Entry<K, V>> i = entrySet().iterator();
            while (i.hasNext()) {
                Entry<K, V> e = i.next();
                K key = e.getKey();
                V value = e.getValue();
                if (value == null) {
                    if (!(m.get(key) == null && m.containsKey(key)))
                        return false;
                } else {
                    if (!value.equals(m.get(key)))
                        return false;
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int h = 0;
        Iterator<Entry<K, V>> i = entrySet().iterator();
        while (i.hasNext())
            h += i.next().hashCode();
        return h;
    }

    @Override
    public V put(K key, V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<K> keySet() {
        return new KeySet<K>();
    }

    @Override
    public Collection<V> values() {
        return new ValueCollection<V>();
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return new EntrySet();
    }

    private class KeyValueIterator<E> implements Iterator<E> {
        boolean key;
        int     cursor;      // index of next element to return
        int     lastRet = -1; // index of last element returned; -1 if no such

        public KeyValueIterator(boolean key) {
            this.key = key;
        }

        @Override
        public boolean hasNext() {
            return cursor != AbstractArrayMap.this.size();
        }

        @SuppressWarnings("unchecked")
        @Override
        public E next() {
            int i = cursor;
            if (i >= AbstractArrayMap.this.size())
                throw new NoSuchElementException();
            cursor = i + 1;
            lastRet = i;

            return key ? (E) AbstractArrayMap.this.key(lastRet) : (E) AbstractArrayMap.this.value(lastRet);
        }

        @Override
        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();

            try {
                AbstractArrayMap.this.remove(key(lastRet));
                cursor = lastRet;
                lastRet = -1;
            } catch (IndexOutOfBoundsException ex) {
                throw new ConcurrentModificationException();
            }
        }

    }

    private class KeySet<E> extends AbstractSet<E> {

        @Override
        public Iterator<E> iterator() {
            return new KeyValueIterator<E>(true);
        }

        @Override
        public boolean contains(Object o) {
            return AbstractArrayMap.this.containsKey(o);
        }

        @Override
        public int size() {
            return AbstractArrayMap.this.size();
        }
    }

    private class ValueCollection<E> extends AbstractCollection<E> {

        @Override
        public Iterator<E> iterator() {
            return new KeyValueIterator<E>(false);
        }

        @Override
        public int size() {
            return AbstractArrayMap.this.size();
        }
    }

    private class EntryIterator implements Iterator<java.util.Map.Entry<K, V>> {

        int cursor;      // index of next element to return
        int lastRet = -1; // index of last element returned; -1 if no such

        @Override
        public boolean hasNext() {
            return cursor != AbstractArrayMap.this.size();
        }

        @Override
        public Entry<K, V> next() {
            int i = cursor;
            if (i >= AbstractArrayMap.this.size())
                throw new NoSuchElementException();
            cursor = i + 1;
            lastRet = i;
            return new SimpleImmutableEntry<K, V>(key(lastRet), value(lastRet));
        }

        @Override
        public void remove() {
            if (lastRet < 0)
                throw new IllegalStateException();

            try {
                AbstractArrayMap.this.remove(key(lastRet));
                cursor = lastRet;
                lastRet = -1;
            } catch (IndexOutOfBoundsException ex) {
                throw new ConcurrentModificationException();
            }
        }
    }

    private class EntrySet extends AbstractSet<java.util.Map.Entry<K, V>> {
        @Override
        public Iterator<java.util.Map.Entry<K, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public int size() {
            return AbstractArrayMap.this.size();
        }
    }

    protected int hashCode(Object o) {
        return o == null ? 0 : rehash(o.hashCode());
    }

    protected int rehash(int hash) {
        return hash;
    }

}
