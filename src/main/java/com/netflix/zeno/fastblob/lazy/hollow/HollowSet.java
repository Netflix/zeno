package com.netflix.zeno.fastblob.lazy.hollow;


public class HollowSet extends HollowCollection {

    public HollowOrdinalIterator ordinalIterator() {
        return new HollowListOrdinalIterator(data, position);
    }
}
