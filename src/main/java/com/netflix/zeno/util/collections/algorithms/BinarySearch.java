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
 *
 * Flavor of BinarySearch algorithm which works with Array interface
 *
 * @author tvaliulin
 *
 */
public class BinarySearch {
    /**
     * Checks that {@code fromIndex} and {@code toIndex} are in the range and
     * throws an appropriate exception, if they aren't.
     */
    private static void rangeCheck(int length, int fromIndex, int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        if (fromIndex < 0) {
            throw new ArrayIndexOutOfBoundsException(fromIndex);
        }
        if (toIndex > length) {
            throw new ArrayIndexOutOfBoundsException(toIndex);
        }
    }

    /**
     * Searches a range of the specified array for the specified object using
     * the binary search algorithm. The range must be sorted into ascending
     * order according to the specified comparator (as by the
     * {@link #sort(Object[], int, int, Comparator) sort(T[], int, int,
     * Comparator)} method) prior to making this call. If it is not sorted, the
     * results are undefined. If the range contains multiple elements equal to
     * the specified object, there is no guarantee which one will be found.
     *
     * @param a
     *            the array to be searched
     * @param fromIndex
     *            the index of the first element (inclusive) to be searched
     * @param toIndex
     *            the index of the last element (exclusive) to be searched
     * @param key
     *            the value to be searched for
     * @param c
     *            the comparator by which the array is ordered. A <tt>null</tt>
     *            value indicates that the elements' {@linkplain Comparable
     *            natural ordering} should be used.
     * @return index of the search key, if it is contained in the array within
     *         the specified range; otherwise,
     *         <tt>(-(<i>insertion point</i>) - 1)</tt>. The <i>insertion
     *         point</i> is defined as the point at which the key would be
     *         inserted into the array: the index of the first element in the
     *         range greater than the key, or <tt>toIndex</tt> if all elements
     *         in the range are less than the specified key. Note that this
     *         guarantees that the return value will be &gt;= 0 if and only if
     *         the key is found.
     * @throws ClassCastException
     *             if the range contains elements that are not <i>mutually
     *             comparable</i> using the specified comparator, or the search
     *             key is not comparable to the elements in the range using this
     *             comparator.
     * @throws IllegalArgumentException
     *             if {@code fromIndex > toIndex}
     * @throws ArrayIndexOutOfBoundsException
     *             if {@code fromIndex < 0 or toIndex > a.length}
     * @since 1.6
     */
    public static <T> int binarySearch(Sortable<T> a, int fromIndex, int toIndex, T key, Comparator<? super T> c) {
        rangeCheck(a.size(), fromIndex, toIndex);
        return binarySearch0(a, fromIndex, toIndex, key, c);
    }

    public static <T> int binarySearch(Sortable<T> a, T key, Comparator<? super T> c) {
        return binarySearch(a, 0, a.size(), key, c);
    }

    // Like public version, but without range checks.
    private static <T> int binarySearch0(Sortable<T> a, int fromIndex, int toIndex, T key, Comparator<? super T> c) {
        if (c == null) {
            throw new NullPointerException();
        }
        int low = fromIndex;
        int high = toIndex - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = a.at(mid);
            int cmp = c.compare(midVal, key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1); // key not found.
    }

}
