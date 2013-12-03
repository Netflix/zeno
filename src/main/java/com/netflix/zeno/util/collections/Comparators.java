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
package com.netflix.zeno.util.collections;

import java.util.Comparator;

/**
 * Registry of comparators being used in the library
 *
 * @author tvaliulin
 *
 */
public class Comparators {
    private static final Comparator<Object> hashCodeComparator = new Comparator<Object>() {
                                                                   @Override
                                                                   public int compare(Object o1, Object o2) {
                                                                       if (o1 == o2) {
                                                                           return 0;
                                                                       }
                                                                       if (o1 == null) {
                                                                           return -1;
                                                                       }
                                                                       if (o2 == null) {
                                                                           return 1;
                                                                       }
                                                                       int hash1 = o1.hashCode();
                                                                       int hash2 = o2.hashCode();
                                                                       return hash1 < hash2 ? -1 : (hash1 > hash2 ? 1 : 0);
                                                                   }
                                                               };

    @SuppressWarnings("unchecked")
    public static <K> Comparator<K> hashCodeComparator() {
        return (Comparator<K>) hashCodeComparator;
    }

    private static final Comparator<?> comparableComparator = new Comparator<Object>() {
                                                                @Override
                                                                @SuppressWarnings("unchecked")
                                                                public int compare(Object o1, Object o2) {
                                                                    if (o1 == o2) {
                                                                        return 0;
                                                                    }
                                                                    if (o1 == null) {
                                                                        return -1;
                                                                    }
                                                                    if (o2 == null) {
                                                                        return 1;
                                                                    }
                                                                    return ((Comparable<Object>) o1).compareTo(o2);
                                                                }
                                                            };

    @SuppressWarnings("unchecked")
    public static <K> Comparator<K> comparableComparator() {
        return (Comparator<K>) comparableComparator;
    }
}
