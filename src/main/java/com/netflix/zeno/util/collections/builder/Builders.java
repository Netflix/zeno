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
package com.netflix.zeno.util.collections.builder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Java Utils containers builders
 *
 * @author tvaliulin
 *
 */
public class Builders {

    public static class HashMapBuilder<K, V> implements MapBuilder<K, V> {

        HashMap<K, V> map;

        @Override
        public void builderInit(int size) {
            this.map = new HashMap<K, V>(size);
        }

        @Override
        public void builderPut(int index, K key, V value) {
            map.put(key, value);
        }

        @Override
        public Map<K, V> builderFinish() {
            return map;
        }
    }

    public static class TreeMapBuilder<K, V> implements MapBuilder<K, V> {

        TreeMap<K, V> map;

        @Override
        public void builderInit(int size) {
            this.map = new TreeMap<K, V>();
        }

        @Override
        public void builderPut(int index, K key, V value) {
            map.put(key, value);
        }

        @Override
        public SortedMap<K, V> builderFinish() {
            return map;
        }
    }

    public static class ArrayListBuilder<E> implements ListBuilder<E> {
        List<E> list;

        @Override
        public void builderInit(int size) {
            list = new ArrayList<E>(size);
        }

        @Override
        public void builderSet(int index, E element) {
            list.add(element);
        }

        @Override
        public List<E> builderFinish() {
            return list;
        }
    }

    public static class HashSetBuilder<E> implements SetBuilder<E> {
        Set<E> set;

        @Override
        public void builderInit(int size) {
            set = new HashSet<E>(size);
        }

        @Override
        public void builderSet(int index, E element) {
            set.add(element);
        }

        @Override
        public Set<E> builderFinish() {
            return set;
        }
    }
}
