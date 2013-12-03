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

/**
 * Utility methods
 *
 * @author tvaliulin
 *
 */
public class Utils {

    /**
     * Array utils
     *
     */
    public final static class Array {
        public static final void swap(Object[] keysAndValues, int x, int y) {
            Object key = keysAndValues[x * 2];
            Object value = keysAndValues[x * 2 + 1];
            keysAndValues[x * 2] = keysAndValues[y * 2];
            keysAndValues[x * 2 + 1] = keysAndValues[y * 2 + 1];
            keysAndValues[y * 2] = key;
            keysAndValues[y * 2 + 1] = value;
        }
    }

    public static final boolean equal(Object o1, Object o2) {
        if (o1 == null && o2 == null)
            return true;
        if (o1 == null)
            return false;
        return o1.equals(o2);
    }
}
