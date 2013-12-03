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

import java.util.Comparator;
import java.util.Map;

/**
 * Immutable BinarySearch map with the hashCodeComparator - this comparator does
 * not mandate for the keys to be comparable
 *
 * @author tvaliulin
 *
 * @param <K>
 * @param <V>
 */
public class BinarySearchArrayHashMap<K, V> extends BinarySearchArrayMap<K, V> {

    public BinarySearchArrayHashMap() {
        super();
    }

    public BinarySearchArrayHashMap(Map<K, V> map) {
        super(map);
    }

    public BinarySearchArrayHashMap(Map.Entry<K, V>[] entries) {
        super(entries);
    }

    @Override
    public Comparator<K> comparator() {
        return Comparators.hashCodeComparator();
    }
}
