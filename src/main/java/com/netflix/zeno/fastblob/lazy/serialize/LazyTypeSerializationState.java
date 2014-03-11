package com.netflix.zeno.fastblob.lazy.serialize;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.state.FreeOrdinalTracker;

public class LazyTypeSerializationState {

    private final LazyOrdinalMap lazyOrdinalMap;
    private final OrdinalMapping ordinalMapping;

    public LazyTypeSerializationState() {
        this.lazyOrdinalMap = new LazyOrdinalMap();
        this.ordinalMapping = new OrdinalMapping();
    }

    public LazyTypeSerializationState(FreeOrdinalTracker freeOrdinalTracker) {
        this.lazyOrdinalMap = new LazyOrdinalMap();
        this.ordinalMapping = new OrdinalMapping(freeOrdinalTracker);
    }

    public void addRecord(int ordinal, ByteDataBuffer buf) {
        int lazyOrdinal = ordinalMapping.assignNewOrdinal(ordinal);
        lazyOrdinalMap.add(lazyOrdinal, buf);
    }

    public void addRecordWithPredefinedLazyOrdinal(int ordinal, int lazyOrdinal, ByteDataBuffer buf) {
        ordinalMapping.assignPredefinedOrdinal(ordinal, lazyOrdinal);
        lazyOrdinalMap.add(lazyOrdinal, buf);
    }

    public void removeRecord(int ordinal) {
        int lazyOrdinal = ordinalMapping.getOrdinal(ordinal);
        lazyOrdinalMap.remove(lazyOrdinal);
    }

    public int getMappedOrdinal(int fromOrdinal) {
        return ordinalMapping.getOrdinal(fromOrdinal);
    }

    public FreeOrdinalTracker getFreeOrdinalTracker() {
        return ordinalMapping.getFreeOrdinalTracker();
    }

}
