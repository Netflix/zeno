package com.netflix.zeno.fastblob.lazy.hollow;

public interface HollowOrdinalIterator {

    public static final int NO_MORE_ORDINALS = Integer.MAX_VALUE;

    public int next();

}
