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
package com.netflix.zeno.fastblob;

import java.util.HashMap;
import java.util.Map;

/**
 * Utils for packing boolean arrays to integer, this is typically used for
 * passing in image information
 *
 * @author timurua
 *
 */
public class FastBlobImageUtils {

    /**
     * Packs boolean array to integer, the booleans with greater indices go
     * first
     *
     * @param a
     * @return
     */
    public static int toInteger(boolean... a) {
        if (a.length > 32) {
            throw new IllegalArgumentException("while packing boolean array in int, the array length should be less than 32");
        }

        int n = 0;
        for (int i = (a.length - 1); i >= 0; --i) {
            n = (n << 1) | (a[i] ? 1 : 0);
        }
        return n;
    }

    public static final int ONE_TRUE = toInteger(true);

    private static final Map<Integer, Integer> ALL_TRUE_PRIVATE = new HashMap<Integer, Integer>();
    static {
        for (int i = 0; i < 32; i++) {
            boolean[] a = new boolean[i];
            for (int j = 0; j < i; j++) {
                a[j] = true;
            }
            ALL_TRUE_PRIVATE.put(i, toInteger(a));
        }
    }

    /**
     * Returns the integer which corresponds to the boolean array of specified
     * length, where all the items are set to true
     *
     * @param count
     * @return
     */
    public static final int getAllTrue(int count) {
        if (count > 32) {
            throw new IllegalArgumentException("while packing boolean array in int, the array length should be less than 32");
        }
        return ALL_TRUE_PRIVATE.get(count);
    }

}
