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
package com.netflix.zeno.util.collections.algorithms;

/**
 * Interface to array-like structures. It allow using those structures with
 * Binary Search and quick sort algorithms
 *
 * @author tvaliulin
 *
 * @param <V>
 */
public interface Sortable<V> {
    V at(int index);

    void swap(int i1, int i2);

    int size();
}