package com.netflix.zeno.fastblob.state.compressed;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.state.compressed.ByteSequenceRetainer.ByteSequenceRetainerIterator;

import java.util.BitSet;

public class FastBlobTypeByteSequenceState {

    private ByteSequenceRetainer thisCycleRetainer;
    private ByteSequenceRetainer nextCycleRetainer;

    /*private String thisCycleVersion;
    private String nextCycleVersion;*/

    public FastBlobTypeByteSequenceState() {
        this.thisCycleRetainer = new ByteSequenceRetainer();
        this.nextCycleRetainer = new ByteSequenceRetainer();
    }

    public void add(int ordinal, ByteData data, long pointer, int objectLength) {
        nextCycleRetainer.addByteSequence(ordinal, data, pointer, objectLength);
    }

    public int get(int ordinal, ByteDataBuffer writeTo) {
        return thisCycleRetainer.retrieveSequence(ordinal, writeTo);
    }

    public void prepareForDelta(BitSet removedOrdinals) {
        ByteSequenceRetainerIterator iterator = thisCycleRetainer.iterator();

        while(iterator.nextKey()) {
            if(!removedOrdinals.get(iterator.getCurrentOrdinal())) {
                iterator.copyEntryTo(nextCycleRetainer);
            }
        }
    }

    public void flip(String nextCycle) {
        ByteSequenceRetainer tempRetainer = thisCycleRetainer;
        thisCycleRetainer = nextCycleRetainer;
        nextCycleRetainer = tempRetainer;
    }

}
