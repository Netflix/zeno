package com.netflix.zeno.fastblob.lazy.serialize;

import com.netflix.zeno.fastblob.state.FreeOrdinalTracker;

import java.util.Arrays;

public class OrdinalMapping {

    private final FreeOrdinalTracker freeOrdinalTracker;
    private int ordinalMapping[];

    public OrdinalMapping() {
        this.ordinalMapping = new int[256];
        this.freeOrdinalTracker = new FreeOrdinalTracker();
    }

    public OrdinalMapping(FreeOrdinalTracker freeOrdinalTracker) {
        this.ordinalMapping = new int[256];
        this.freeOrdinalTracker = freeOrdinalTracker;
    }

    public int getOrdinal(int fromOrdinal) {
        return ordinalMapping[fromOrdinal];
    }

    public int assignNewOrdinal(int fromOrdinal) {
        ensureCapacity(fromOrdinal);
        int newOrdinal = freeOrdinalTracker.getFreeOrdinal();
        ordinalMapping[fromOrdinal] = newOrdinal;
        return newOrdinal;
    }

    public void releaseOrdinal(int fromOrdinal) {
        freeOrdinalTracker.returnOrdinalToPool(ordinalMapping[fromOrdinal]);
    }

    public void assignPredefinedOrdinal(int fromOrdinal, int toOrdinal) {
        ensureCapacity(fromOrdinal);
        ordinalMapping[fromOrdinal] = toOrdinal;
    }

    public FreeOrdinalTracker getFreeOrdinalTracker() {
        return freeOrdinalTracker;
    }

    private void ensureCapacity(int ordinal) {
        while(ordinal >= ordinalMapping.length) {
            ordinalMapping = Arrays.copyOf(ordinalMapping, ordinalMapping.length * 2);
        }
    }

}
