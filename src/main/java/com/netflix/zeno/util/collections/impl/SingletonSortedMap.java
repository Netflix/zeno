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


import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Implementation of Singleton SortedMap
 *
 * @author tvaliulin
 *
 * @param <K>
 * @param <V>
 */
public class SingletonSortedMap<K, V> extends AbstractMap<K, V> implements SortedMap<K, V>, Serializable {

    private static final long serialVersionUID = 4009578255191820277L;

    private final K           key;
    private final V           value;

    public SingletonSortedMap(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean containsKey(Object object) {
        return Utils.equal(this.key, object);
    }

    @Override
    public boolean containsValue(Object object) {
        return Utils.equal(this.value, object);
    }

    @Override
    public V get(Object object) {
        return Utils.equal(this.key, object) ? this.value : null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public Set<K> keySet() {
        return java.util.Collections.singleton(this.key);
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public Collection<V> values() {
        return java.util.Collections.singleton(this.value);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return java.util.Collections.singleton((Map.Entry<K, V>) new SimpleImmutableEntry(key, value));
    }

    @Override
    public Comparator<? super K> comparator() {
        return comparator();
    }

    @Override
    public K firstKey() {
        return this.key;
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return (comparator().compare(this.key, toKey) < 0) ? this : NetflixCollections.<K, V> emptySortedMap();
    }

    @Override
    public K lastKey() {
        return this.key;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        Comparator<K> comparator = (Comparator<K>) comparator();

        return ((comparator.compare(this.key, toKey) < 0) && (comparator.compare(this.key, fromKey) >= 0)) ? this : NetflixCollections.<K, V> emptySortedMap();
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return (comparator().compare(this.key, fromKey) >= 0) ? this : NetflixCollections.<K, V> emptySortedMap();
    }
}