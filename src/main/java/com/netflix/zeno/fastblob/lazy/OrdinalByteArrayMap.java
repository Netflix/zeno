package com.netflix.zeno.fastblob.lazy;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.SegmentedByteArray;

import java.util.Arrays;

public class OrdinalByteArrayMap {

    private static final long EMPTY_BUCKET_VALUE = -1;
    private static final int ORDINAL_SHIFT = 36;
    private static final long POINTER_MASK = (1L << ORDINAL_SHIFT) - 1;

    private long ordinalsAndPointers[];
    private final SegmentedByteArray byteData;
    private int size;
    private int capacity;
    private long dataLength = 0;

    public OrdinalByteArrayMap(int initialElements) {
        int arraySize = 1 << (32 - Integer.numberOfLeadingZeros((initialElements * 10 / 7) - 1));
        this.ordinalsAndPointers = new long[arraySize];
        this.capacity = ordinalsAndPointers.length * 7 / 10;
        this.size = 0;
        this.byteData = new SegmentedByteArray(13);

        Arrays.fill(ordinalsAndPointers, EMPTY_BUCKET_VALUE);
    }

    public void add(int ordinal, ByteData data, long start, int length) {
        if(size == capacity)
            growKeyArray();

        int bucket = rehash(ordinal) % ordinalsAndPointers.length;

        while(ordinalsAndPointers[bucket] != EMPTY_BUCKET_VALUE) {
            bucket++;
            bucket %= ordinalsAndPointers.length;
        }

        long pointer = dataLength;

        copyData(data, start, length);

        long key = ((long)ordinal << ORDINAL_SHIFT) | pointer;

        ordinalsAndPointers[bucket] = key;
        size++;
    }

    public long getPointer(int ordinal) {
        int bucket = rehash(ordinal) % ordinalsAndPointers.length;
        while(ordinalsAndPointers[bucket] != EMPTY_BUCKET_VALUE) {
            int bucketOrdinal = (int)(ordinalsAndPointers[bucket] >>> ORDINAL_SHIFT);

            if(bucketOrdinal == ordinal)
                return ordinalsAndPointers[bucket] & POINTER_MASK;

            bucket++;
            bucket %= ordinalsAndPointers.length;
        }

        return -1;
    }

    private void growKeyArray() {
        long newOrdinalsAndPointers[] = new long[ordinalsAndPointers.length * 2];
        Arrays.fill(newOrdinalsAndPointers, EMPTY_BUCKET_VALUE);

        Arrays.sort(ordinalsAndPointers);

        for(int i=0;i<ordinalsAndPointers.length;i++) {
            if(ordinalsAndPointers[i] != EMPTY_BUCKET_VALUE) {
                int newBucket = rehash((int)(ordinalsAndPointers[i] >>> ORDINAL_SHIFT)) % newOrdinalsAndPointers.length;
                while(newOrdinalsAndPointers[newBucket] != EMPTY_BUCKET_VALUE)
                    newBucket = (newBucket + 1) % newOrdinalsAndPointers.length;
                newOrdinalsAndPointers[newBucket] = ordinalsAndPointers[i];
            }
        }

        ordinalsAndPointers = newOrdinalsAndPointers;
        capacity = ordinalsAndPointers.length * 7 / 10;
    }

    public ByteData getByteData() {
        return byteData;
    }

    private void copyData(ByteData data, long start, int length) {
        long end = start + length;
        for(long i=start;i<end;i++) {
            byteData.set((int)(dataLength++), data.get((int)i));
        }
    }


    private int rehash(int hash) {
        hash = ~hash + (hash << 15);
        hash = hash ^ (hash >>> 12);
        hash = hash + (hash << 2);
        hash = hash ^ (hash >>> 4);
        hash = hash * 2057;
        hash = hash ^ (hash >>> 16);
        return hash & Integer.MAX_VALUE;
    }

}
