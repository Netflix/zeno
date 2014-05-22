package com.netflix.zeno.fastblob;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FastBlobUtils {
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
    public static final Map<Integer, Integer> ALL_TRUE = Collections.unmodifiableMap(ALL_TRUE_PRIVATE);

}
