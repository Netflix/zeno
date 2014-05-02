package com.netflix.zeno.fastblob.state.compressed;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.VarInt;

import java.util.Arrays;

public class ByteSequenceRetainer {

    private final static long EMPTY_BUCKET_VALUE = -1L;

    private long keys[];

    private final ByteDataBuffer buf;

    private int size;
    private int sizeBeforeGrow;

    public ByteSequenceRetainer() {
        this.buf = new ByteDataBuffer(262144);
        this.keys = new long[256];
        Arrays.fill(keys, EMPTY_BUCKET_VALUE);
        this.size = 0;
        this.sizeBeforeGrow = 179;
    }

    /*public void addByteSequence(int ordinal, ByteDataBuffer sequence) {
        addByteSequence(ordinal, sequence.getUnderlyingArray(), 0L, (int)sequence.length());
    }*/

    public void addByteSequence(int ordinal, ByteData readSequenceFrom, long seqPointer, int seqLength) {
        if(size == sizeBeforeGrow)
            growKeyArray();

        long key = ((long)ordinal << 36 | buf.length());
        int bucket = hashInt(ordinal) % keys.length;

        while(keys[bucket] != EMPTY_BUCKET_VALUE) {
            bucket++;
            bucket %= keys.length;
        }

        VarInt.writeVInt(buf, seqLength);
        buf.copyFrom(readSequenceFrom, seqPointer, seqLength);
        keys[bucket] = key;
        size++;
    }

    /**
     * Writes the retained sequence of bytes to <code>writeTo</code>.  Returns the number of bytes written.
     */
    public int retrieveSequence(int ordinal, ByteDataBuffer writeTo) {
        int bucket = hashInt(ordinal) % keys.length;

        while(keys[bucket] != EMPTY_BUCKET_VALUE) {
            int foundOrdinal = (int)(keys[bucket] >>> 36);
            if(foundOrdinal == ordinal) {
                long pointer = keys[bucket] & 0xFFFFFFFFFL;
                int length = VarInt.readVInt(buf.getUnderlyingArray(), pointer);
                pointer += VarInt.sizeOfVInt(length);
                writeTo.copyFrom(buf.getUnderlyingArray(), pointer, length);
                return length;
            }
        }

        return 0;
    }

    public ByteSequenceRetainerIterator iterator() {
        return new ByteSequenceRetainerIterator(keys, size, buf.getUnderlyingArray());
    }

    public void clear() {
        size = 0;
        Arrays.fill(keys, EMPTY_BUCKET_VALUE);
        buf.reset();
    }

    private void growKeyArray() {
        long newKeys[] = new long[keys.length * 2];

        Arrays.fill(newKeys, EMPTY_BUCKET_VALUE);

        long keysToAdd[] = sortedPopulatedKeysArray(keys, size);

        populateNewHashArray(newKeys, keysToAdd);

        this.keys = newKeys;
        this.sizeBeforeGrow = (newKeys.length * 7) / 10;
    }

    /**
     * Hash all of the existing values specified by the keys in the supplied long array
     * into the supplied AtomicLongArray.
     */
    private void populateNewHashArray(long[] newKeys, long[] valuesToAdd) {
        int modBitmask = newKeys.length - 1;

        for(int i=0;i<valuesToAdd.length;i++) {
            int ordinal = (int)(valuesToAdd[i] >>> 36);
            int hash = hashInt(ordinal);

            int bucket = hash & modBitmask;
            while(newKeys[bucket] != EMPTY_BUCKET_VALUE)
                bucket = (bucket + 1) & modBitmask;
            newKeys[bucket] = valuesToAdd[i];
        }
    }

    private int hashInt(int hash) {
        hash = ~hash + (hash << 15);
        hash = hash ^ (hash >>> 12);
        hash = hash + (hash << 2);
        hash = hash ^ (hash >>> 4);
        hash = hash * 2057;
        hash = hash ^ (hash >>> 16);
        return hash & Integer.MAX_VALUE;
    }


    static class ByteSequenceRetainerIterator {
        private final long keysToAdd[];
        private final ByteData dataArray;

        private int currentKeyIndex;

        private int currentOrdinal;
        private int currentDataSize;
        private long currentPointer;

        private ByteSequenceRetainerIterator(long keyArray[], int size, ByteData dataArray) {
            this.keysToAdd = sortedPopulatedKeysArray(keyArray, size);
            this.dataArray = dataArray;
            this.currentKeyIndex = -1;
        }

        public boolean nextKey() {
            currentKeyIndex++;
            if(currentKeyIndex < keysToAdd.length) {
                currentOrdinal = (int) (keysToAdd[currentKeyIndex] >> 36);
                currentPointer = keysToAdd[currentKeyIndex] & 0xFFFFFFFFFL;
                currentDataSize = VarInt.readVInt(dataArray, currentPointer);
                currentPointer += VarInt.sizeOfVInt(currentDataSize);
                return true;
            }
            return false;
        }

        public int getCurrentOrdinal() {
            return currentOrdinal;
        }

        public int getCurrentDataSize() {
            return currentDataSize;
        }

        public long getCurrentPointer() {
            return currentPointer;
        }

        public void copyEntryTo(ByteSequenceRetainer other) {
            other.addByteSequence(currentOrdinal, dataArray, currentPointer, currentDataSize);
        }

    }

    private static long[] sortedPopulatedKeysArray(long[] keys, int size) {
        long arr[] = new long[size];

        int counter = 0;

        /// do not iterate over these values in the same order in which they appear in the hashed array.
        /// if we do so, we cause large clusters of collisions to appear (because we resolve collisions with linear probing).
        for(int i=0;i<keys.length;i++) {
            if(keys[i] != EMPTY_BUCKET_VALUE) {
                arr[counter++] = keys[i];
            }
        }

        Arrays.sort(arr);

        return arr;
    }

}
