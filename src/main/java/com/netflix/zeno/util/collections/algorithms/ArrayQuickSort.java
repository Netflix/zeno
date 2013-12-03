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

import java.util.Comparator;

/**
 * A simple quicksort implementation.
 *
 * @author dkoszewnik
 *
 */
public class ArrayQuickSort {

    private static int nextPivot = 0;

    public static <E> void sort(Sortable<E> arr, Comparator<E> comparator) {
        nextPivot = 0;
        quicksort(arr, comparator, 0, arr.size() - 1);
    }

    private static <E> void quicksort(Sortable<E> arr, Comparator<E> comparator, int from, int to) {
        if(to > from) {
            int pivotIndex = findPivot(from, to);
            pivotIndex = pivot(arr, comparator, from, to, pivotIndex);
            quicksort(arr, comparator, from, pivotIndex - 1);
            quicksort(arr, comparator, pivotIndex + 1, to);
        }
    }

    private static <E> int findPivot(int from, int to) {
        return (++nextPivot % ((to - from) + 1)) + from;
    }

    private static <E> int pivot(Sortable<E> arr, Comparator<E> comparator, int from, int to, int pivotIndex) {
        E pivotValue = arr.at(pivotIndex);
        arr.swap(pivotIndex, to);

        for(int i=from;i<to;i++) {
            if(comparator.compare(arr.at(i), pivotValue) <= 0) {
                arr.swap(i, from);
                from++;
            }
        }

        arr.swap(from, to);
        return from;
    }

}

