package com.netflix.zeno.fastblob.lazy.serialize;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;

import java.util.Arrays;
import java.util.BitSet;

public class LazyOrdinalMap {

    /// high 36 bits are pointer, low 28 bits are length
    private long pointersAndLengths[];
    private final ByteDataBuffer byteData;
    private final BitSet removedOrdinals;

    public LazyOrdinalMap() {
        this.pointersAndLengths = new long[256];
        Arrays.fill(pointersAndLengths, -1);
        this.byteData = new ByteDataBuffer(16384);
        this.removedOrdinals = new BitSet();

    }

    public void add(int ordinal, ByteDataBuffer data) {
        ensurePointersCapacity(ordinal);
        long currentPointer = byteData.length();

        byteData.copyFrom(data.getUnderlyingArray(), 0L, (int)data.length());

        pointersAndLengths[ordinal] = currentPointer << 28 | data.length();
    }

    public void remove(int ordinal) {
        removedOrdinals.set(ordinal);
    }

    public void get(int ordinal, ByteDataBuffer writeTo) {
        long pointer = pointersAndLengths[ordinal] >>> 28;
        int length = (int)(pointersAndLengths[ordinal] & 0xFFFFFFFL);

        writeTo.copyFrom(byteData.getUnderlyingArray(), pointer, length);
    }

    public void compactByteData() {
        int numPopulatedRecords = 0;

        for(int i=0;i<pointersAndLengths.length;i++) {
            if(pointersAndLengths[i] != -1)
                numPopulatedRecords++;
        }

        long pointersAndOrdinals[] = new long[numPopulatedRecords];
        int counter = 0;

        for(int i=0;i<pointersAndLengths.length;i++) {
            if(pointersAndLengths[i] != -1)
                pointersAndOrdinals[counter++] = (pointersAndLengths[i] & 0xFFFFFFFFF0000000L) | i;
        }

        Arrays.sort(pointersAndOrdinals);

        long currentCopyToPointer = 0;

        for(int i=0;i<pointersAndOrdinals.length;i++) {
            int ordinal = (int)(pointersAndOrdinals[i] & 0xFFFFFFFL);

            if(!removedOrdinals.get(ordinal)) {
                int length = (int)(pointersAndLengths[ordinal] & 0xFFFFFFFL);
                long copyFromPointer = pointersAndOrdinals[i] >>> 28;

                if(copyFromPointer != currentCopyToPointer)
                    byteData.getUnderlyingArray().copy(byteData.getUnderlyingArray(), copyFromPointer, currentCopyToPointer, length);

                pointersAndLengths[ordinal] = currentCopyToPointer << 28 | length;

                currentCopyToPointer += length;
            } else {
                pointersAndLengths[ordinal] = -1;
            }
        }

        byteData.setPosition(currentCopyToPointer);
        removedOrdinals.clear();
    }

    long lengthOfByteData() {
        return byteData.length();
    }

    private void ensurePointersCapacity(int ordinal) {
        while(pointersAndLengths.length <= ordinal) {
            int oldLength = pointersAndLengths.length;
            pointersAndLengths = Arrays.copyOf(pointersAndLengths, oldLength * 2);
            Arrays.fill(pointersAndLengths, oldLength, pointersAndLengths.length, -1);
        }
    }
}
