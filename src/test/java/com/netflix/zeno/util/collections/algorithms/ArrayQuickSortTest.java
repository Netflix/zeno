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

import com.netflix.zeno.util.collections.Comparators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

public class ArrayQuickSortTest {

    private static final int NUM_ITERATIONS = 1000;
    private static final int LIST_SIZE = 1000;
    private static final Random rand = new Random();

    @Test
    public void testRandomData() {
        for(int i=0;i<NUM_ITERATIONS;i++) {
            IntegerArray arr = createShuffledArray(LIST_SIZE);

            Comparator<Integer> comparator = Comparators.comparableComparator();

            ArrayQuickSort.sort(arr, comparator);

            assertSorted(arr);
        }
    }

    private IntegerArray createShuffledArray(int size) {
        IntegerArray arr = new IntegerArray(size);

        for(int i=0;i<LIST_SIZE;i++) {
            arr.add(Integer.valueOf(rand.nextInt()));
        }

        return arr;
    }

    private void assertSorted(IntegerArray arr) {
        Integer currentValue = arr.get(0);

        for(int i=1;i<arr.size();i++) {
            Assert.assertTrue(currentValue.intValue() <= arr.get(i).intValue());
            currentValue = arr.get(i);
        }
    }

    @SuppressWarnings("serial")
    public class IntegerArray extends ArrayList<Integer> implements Sortable<Integer> {

        public IntegerArray(int size) {
            super(size);
        }

        @Override
        public Integer at(int index) {
            return get(index);
        }

        @Override
        public void swap(int i1, int i2) {
            Integer temp = get(i1);
            set(i1, get(i2));
            set(i2, temp);
        }

    }

}
