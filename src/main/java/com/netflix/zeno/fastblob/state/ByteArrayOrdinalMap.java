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
package com.netflix.zeno.fastblob.state;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLongArray;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.SegmentedByteArray;
import com.netflix.zeno.fastblob.record.SegmentedByteArrayHasher;
import com.netflix.zeno.fastblob.record.VarInt;

/**
 *
 * This data structure maps byte sequences to ordinals.  This is a hash table.  The <code>pointersAndOrdinals</code> AtomicLongArray contains
 * keys, and the <code>ByteDataBuffer</code> contains values.  Each key has two components.  The high 32 bits in the key represents the ordinal.
 * The low 32 bits represents the pointer to the start position of the byte sequence in the ByteDataBuffer.  Each byte sequence is preceded by
 * a variable-length integer (see {@link VarInt}), indicating the length of the sequence.<p/>
 *
 * This implementation is extremely fast.  Even though it would be technically correct and clearer,
 * using a separate int[] array for the pointers, and an AtomicIntegerArray for the ordinals as keys
 * was measured as two orders of magnitude slower.
 *
 * @author dkoszewnik
 *
 */
public class ByteArrayOrdinalMap {

    private final long EMPTY_BUCKET_VALUE = -1L;

    /// IMPORTANT: Thread safety:  We need volatile access semantics to the individual elements in the
    /// pointersAndOrdinals array.  This only works in JVMs 1.5 or later (JSR 133).
    /// Ordinal is the high 32 bits.  Pointer to byte data is the low 32 bits.
    private AtomicLongArray pointersAndOrdinals;
    private final ByteDataBuffer byteData;
    private final FreeOrdinalTracker freeOrdinalTracker;
    private int size;
    private int sizeBeforeGrow;

    private int pointersByOrdinal[];


    public ByteArrayOrdinalMap() {
        this(1048576);
    }

    public ByteArrayOrdinalMap(int bufferSize) {
        this.freeOrdinalTracker = new FreeOrdinalTracker();
        this.byteData = new ByteDataBuffer(bufferSize);
        this.pointersAndOrdinals = emptyKeyArray(256);
        this.sizeBeforeGrow = 179; /// 70% load factor
        this.size = 0;
    }

    private ByteArrayOrdinalMap(long keys[], ByteDataBuffer byteData, FreeOrdinalTracker freeOrdinalTracker, int keyArraySize) {
        this.freeOrdinalTracker = freeOrdinalTracker;
        this.byteData = byteData;
        AtomicLongArray pointersAndOrdinals = emptyKeyArray(keyArraySize);
        populateNewHashArray(pointersAndOrdinals, keys);
        this.pointersAndOrdinals = pointersAndOrdinals;
        this.size = keys.length;
        this.sizeBeforeGrow = keyArraySize * 7 / 10; /// 70% load factor

    }


    /**
     * Add a sequence of bytes to this map.  If the sequence of bytes has already been added to this map, return the originally assigned ordinal.
     * If the sequence of bytes has not been added to this map, assign and return a new ordinal.  This operation is thread-safe.
     */
    public int getOrAssignOrdinal(ByteDataBuffer serializedRepresentation) {
        int hash = SegmentedByteArrayHasher.hashCode(serializedRepresentation);

        int modBitmask = pointersAndOrdinals.length() - 1;
        int bucket = hash & modBitmask;
        long key = pointersAndOrdinals.get(bucket);

        /// linear probing to resolve collisions.
        while(key != EMPTY_BUCKET_VALUE) {
            if(compare(serializedRepresentation, key)) {
                return (int)(key >> 32);
            }

            bucket = (bucket + 1) & modBitmask;
            key = pointersAndOrdinals.get(bucket);
        }

        return assignOrdinal(serializedRepresentation, hash);
    }

    /// acquire the lock before writing.
    private synchronized int assignOrdinal(ByteDataBuffer serializedRepresentation, int hash) {
        if(size > sizeBeforeGrow)
            growKeyArray();

        /// check to make sure that after acquiring the lock, the element still does not exist.
        /// this operation is akin to double-checked locking which is 'fixed' with the JSR 133 memory model in JVM >= 1.5.
        int modBitmask = pointersAndOrdinals.length() - 1;
        int bucket = hash & modBitmask;
        long key = pointersAndOrdinals.get(bucket);

        while(key != EMPTY_BUCKET_VALUE) {
            if(compare(serializedRepresentation, key)) {
                return (int)(key >> 32);
            }

            bucket = (bucket + 1) & modBitmask;
            key = pointersAndOrdinals.get(bucket);
        }

        /// the ordinal for this object still does not exist in the list, even after the lock has been acquired.
        /// it is up to this thread to add it at the current bucket position.
        int ordinal = freeOrdinalTracker.getFreeOrdinal();
        int pointer = byteData.length();

        VarInt.writeVInt(byteData, serializedRepresentation.length());
        serializedRepresentation.copyTo(byteData);

        key = ((long)ordinal << 32) | pointer;

        size++;

        /// this set on the AtomicLongArray has volatile semantics (i.e. behaves like a monitor release).
        /// Any other thread reading this element in the AtomicLongArray will have visibility to all memory writes this thread has made up to this point.
        /// This means the entire byte sequence is guaranteed to be visible to any thread which reads the pointer to that data.
        pointersAndOrdinals.set(bucket, key);

        return ordinal;
    }

    /**
     * Assign a predefined ordinal to a serialized representation.<p/>
     *
     * WARNING: THIS OPERATION IS NOT THREAD-SAFE.<p/>
     *
     * This is intended for use in the client-side heap-safe double snapshot load.
     *
     */
    public void put(ByteDataBuffer serializedRepresentation, int ordinal) {
        if(size > sizeBeforeGrow)
            growKeyArray();

        int hash = SegmentedByteArrayHasher.hashCode(serializedRepresentation);

        int modBitmask = pointersAndOrdinals.length() - 1;
        int bucket = hash & modBitmask;
        long key = pointersAndOrdinals.get(bucket);

        while(key != EMPTY_BUCKET_VALUE) {
            if(compare(serializedRepresentation, key))
                return;

            bucket = (bucket + 1) & modBitmask;
            key = pointersAndOrdinals.get(bucket);
        }

        int pointer = byteData.length();

        VarInt.writeVInt(byteData, serializedRepresentation.length());
        serializedRepresentation.copyTo(byteData);

        key = ((long)ordinal << 32) | pointer;

        size++;

        pointersAndOrdinals.set(bucket, key);
    }

    /**
     * Returns the ordinal for a previously added byte sequence.  If this byte sequence has not been added to the map, then -1 is returned.<p/>
     *
     * This is intended for use in the client-side heap-safe double snapshot load.
     *
     * @param serializedRepresentation
     * @return The ordinal for this serialized representation, or -1.
     */
    public int get(ByteDataBuffer serializedRepresentation) {
        int hash = SegmentedByteArrayHasher.hashCode(serializedRepresentation);

        int modBitmask = pointersAndOrdinals.length() - 1;
        int bucket = hash & modBitmask;
        long key = pointersAndOrdinals.get(bucket);

        /// linear probing to resolve collisions.
        while(key != EMPTY_BUCKET_VALUE) {
            if(compare(serializedRepresentation, key)) {
                return (int)(key >> 32);
            }

            bucket = (bucket + 1) & modBitmask;
            key = pointersAndOrdinals.get(bucket);
        }

        return -1;
    }

    /**
     * Remove all entries from this map, but reuse the existing arrays when populating the map next time.
     *
     * This is intended for use in the client-side heap-safe double snapshot load.
     */
    public void clear() {
        for(int i=0;i<pointersAndOrdinals.length();i++) {
            pointersAndOrdinals.set(i, EMPTY_BUCKET_VALUE);
        }
        byteData.reset();
        size = 0;
    }

    /**
     * Create an array mapping the ordinals to pointers, so that they can be easily looked up
     * when writing to blob streams.
     *
     * @return the maximum length, in bytes, of any byte sequence in this map.
     */
    public int prepareForWrite() {
        int maxOrdinal = 0;
        int maxLength = 0;

        for(int i=0;i<pointersAndOrdinals.length();i++) {
            long key = pointersAndOrdinals.get(i);
            if(key != EMPTY_BUCKET_VALUE) {
                int ordinal = (int)(key >> 32);
                if(ordinal > maxOrdinal)
                    maxOrdinal = ordinal;
            }
        }

        pointersByOrdinal = new int[maxOrdinal + 1];
        Arrays.fill(pointersByOrdinal, -1);

        for(int i=0;i<pointersAndOrdinals.length();i++) {
            long key = pointersAndOrdinals.get(i);
            if(key != EMPTY_BUCKET_VALUE) {
                int ordinal = (int)(key >> 32);
                pointersByOrdinal[ordinal] = (int)key;

                int dataLength = VarInt.readVInt(byteData.getUnderlyingArray(), pointersByOrdinal[ordinal]);
                if(dataLength > maxLength)
                    maxLength = dataLength;
            }
        }

        return maxLength;
    }

    /**
     * Reclaim space in the byte array used in the previous cycle, but not referenced in this cycle.<p/>
     *
     * This is achieved by shifting all used byte sequences down in the byte array, then updating
     * the key array to reflect the new pointers and exclude the removed entries.  This is also where ordinals
     * which are unused are returned to the pool.<p/>
     *
     * @param usedOrdinals a bit set representing the ordinals which are currently referenced by any image.
     */
    public void compact(ThreadSafeBitSet usedOrdinals) {
        long populatedReverseKeys[] = new long[size];

        int counter = 0;

        for(int i=0;i<pointersAndOrdinals.length();i++) {
            long key = pointersAndOrdinals.get(i);
            if(key != EMPTY_BUCKET_VALUE) {
                populatedReverseKeys[counter++] = key << 32 | key >>> 32;
            }
        }

        Arrays.sort(populatedReverseKeys);

        SegmentedByteArray arr = byteData.getUnderlyingArray();
        int currentCopyPointer = 0;

        for(int i=0;i<populatedReverseKeys.length;i++) {
            int ordinal = (int)populatedReverseKeys[i];

            if(usedOrdinals.get(ordinal)) {
                int pointer = (int)(populatedReverseKeys[i] >> 32);
                int length = VarInt.readVInt(arr, pointer);
                length += VarInt.sizeOfVInt(length);

                if(currentCopyPointer != pointer)
                    arr.copy(arr, pointer, currentCopyPointer, length);

                populatedReverseKeys[i] = populatedReverseKeys[i] << 32 | currentCopyPointer;

                currentCopyPointer += length;
            } else {
                freeOrdinalTracker.returnOrdinalToPool(ordinal);
                populatedReverseKeys[i] = EMPTY_BUCKET_VALUE;
            }
        }

        byteData.setPosition(currentCopyPointer);

        for(int i=0;i<pointersAndOrdinals.length();i++) {
            pointersAndOrdinals.set(i, EMPTY_BUCKET_VALUE);
        }

        populateNewHashArray(pointersAndOrdinals, populatedReverseKeys);
        size = usedOrdinals.cardinality();

        pointersByOrdinal = null;
    }

    /**
     * Write the byte sequence of an object specified by an ordinal to the OutputStream.
     *
     * @throws IOException
     */
    public void writeSerializedObject(OutputStream out, int ordinal) throws IOException {
        int pointer = pointersByOrdinal[ordinal];
        int length = VarInt.readVInt(byteData.getUnderlyingArray(), pointer);
        pointer += VarInt.sizeOfVInt(length);

        byteData.getUnderlyingArray().writeTo(out, pointer, length);
    }

    public boolean isReadyForWriting() {
        return pointersByOrdinal != null;
    }

    public boolean isReadyForAddingObjects() {
        return pointersByOrdinal == null;
    }

    public int getDataSize() {
        return byteData.length();
    }

    /**
     * Copy all of the data from this ByteArrayOrdinalMap to the provided FastBlobTypeSerializationState.
     *
     * Image memberships for each ordinal are determined via the provided array of ThreadSafeBitSets.
     *
     * @param copyTo
     * @param imageMemberships
     */
    void copySerializedObjectData(FastBlobTypeSerializationState<?> copyTo, ThreadSafeBitSet imageMemberships[]) {
        ByteDataBuffer buf = new ByteDataBuffer();
        boolean imageMembershipsFlags[] = new boolean[imageMemberships.length];

        for(int i=0;i<pointersAndOrdinals.length();i++) {
            long pointerAndOrdinal = pointersAndOrdinals.get(i);
            if(pointerAndOrdinal != EMPTY_BUCKET_VALUE) {
                int pointer = (int)(pointerAndOrdinal);
                int ordinal = (int)(pointerAndOrdinal >> 32);

                for(int imageIndex=0;imageIndex<imageMemberships.length;imageIndex++) {
                    imageMembershipsFlags[imageIndex] = imageMemberships[imageIndex].get(ordinal);
                }

                int sizeOfData = VarInt.readVInt(byteData.getUnderlyingArray(), pointer);
                pointer += VarInt.sizeOfVInt(sizeOfData);

                for(int j=0;j<sizeOfData;j++) {
                    buf.write(byteData.get(pointer++));
                }

                copyTo.addData(buf, imageMembershipsFlags);
                buf.reset();
            }
        }
    }

    /**
     * Compare the byte sequence contained in the supplied ByteDataBuffer with the
     * sequence contained in the map pointed to by the specified key, byte by byte.
     */
    private boolean compare(ByteDataBuffer serializedRepresentation, long key) {
        int position = (int)key;

        int sizeOfData = VarInt.readVInt(byteData.getUnderlyingArray(), position);

        if(sizeOfData != serializedRepresentation.length())
            return false;

        position += VarInt.sizeOfVInt(sizeOfData);

        for(int i=0;i<sizeOfData;i++) {
            if(serializedRepresentation.get(i) != byteData.get(position++))
                return false;
        }

        return true;
    }

    /**
     * Grow the key array.  All of the values in the current array must be re-hashed and added to the new array.
     */
    private void growKeyArray() {
        AtomicLongArray newKeys = emptyKeyArray(pointersAndOrdinals.length() * 2);

        long valuesToAdd[] = new long[size];

        int counter = 0;

        /// do not iterate over these values in the same order in which they appear in the hashed array.
        /// if we do so, we cause large clusters of collisions to appear (because we resolve collisions with linear probing).
        for(int i=0;i<pointersAndOrdinals.length();i++) {
            long key = pointersAndOrdinals.get(i);
            if(key != EMPTY_BUCKET_VALUE) {
                valuesToAdd[counter++] = key;
            }
        }

        Arrays.sort(valuesToAdd);

        populateNewHashArray(newKeys, valuesToAdd);

        /// 70% load factor
        sizeBeforeGrow = (newKeys.length() * 7) / 10;
        pointersAndOrdinals = newKeys;
    }

    /**
     * Hash all of the existing values specified by the keys in the supplied long array
     * into the supplied AtomicLongArray.
     */
    private void populateNewHashArray(AtomicLongArray newKeys, long[] valuesToAdd) {
        int modBitmask = newKeys.length() - 1;

        for(int i=0;i<valuesToAdd.length;i++) {
            if(valuesToAdd[i] != EMPTY_BUCKET_VALUE) {
                int hash = rehashPreviouslyAddedData(valuesToAdd[i]);
                int bucket = hash & modBitmask;
                while(newKeys.get(bucket) != EMPTY_BUCKET_VALUE)
                    bucket = (bucket + 1) & modBitmask;
                newKeys.set(bucket, valuesToAdd[i]);
            }
        }
    }

    /**
     * Get the hash code for the byte array pointed to by the specified key.
     */
    private int rehashPreviouslyAddedData(long key) {
        int position = (int)key;

        int sizeOfData = VarInt.readVInt(byteData.getUnderlyingArray(), position);
        position += VarInt.sizeOfVInt(sizeOfData);

        return SegmentedByteArrayHasher.hashCode(byteData.getUnderlyingArray(), position, sizeOfData);
    }

    /**
     * Create an AtomicLongArray of the specified size, each value in the array will be EMPTY_BUCKET_VALUE
     */
    private AtomicLongArray emptyKeyArray(int size) {
        AtomicLongArray arr = new AtomicLongArray(size);
        for(int i=0;i<arr.length();i++) {
            arr.set(i, EMPTY_BUCKET_VALUE);
        }
        return arr;
    }

    /**
     * This is used to store the server's SerializationState, so that it may resume the delta chain after a new server is brought back up.
     * @param os
     * @throws IOException
     */
    public void serializeTo(OutputStream os) throws IOException {
        /// indicate which state this ByteArrayOrdinalMap was in.
        int isPreparedForWrite = pointersByOrdinal != null ? 1 : 0;
        os.write(isPreparedForWrite);

        /// write the hashed key array size
        VarInt.writeVInt(os, pointersAndOrdinals.length());


        /// write the keys in sorted ordinal order to the stream
        long keys[] = new long[size];

        int counter = 0;

        for(int i=0;i<pointersAndOrdinals.length();i++) {
            long key = pointersAndOrdinals.get(i);
            if(key != EMPTY_BUCKET_VALUE) {
                keys[counter++] = key;
            }
        }

        Arrays.sort(keys);

        VarInt.writeVInt(os, keys.length);

        for(int i=0;i<keys.length;i++) {
            VarInt.writeVInt(os, (int)(keys[i] >> 32));
            VarInt.writeVInt(os, (int)(keys[i]));
        }

        /// write the byte data to the stream
        VarInt.writeVInt(os, byteData.length());

        for(int i=0;i<byteData.length();i++) {
            os.write(byteData.get(i) & 0xFF);
        }

        /// write the freeOrdinalTracker to the stream
        freeOrdinalTracker.serializeTo(os);
    }

    /**
     * This is used to restore the server's SerializationState, so that it may resume the delta chain after a new server is brought back up.
     *
     * @throws IOException
     */
    public static ByteArrayOrdinalMap deserializeFrom(InputStream is) throws IOException {
        boolean wasPreparedForWrite = is.read() == 1;

        int hashedKeyArraySize = VarInt.readVInt(is);

        long keys[] = new long[VarInt.readVInt(is)];

        for(int i=0;i<keys.length;i++) {
            keys[i] = ((long)VarInt.readVInt(is) << 32) | VarInt.readVInt(is);
        }

        ByteDataBuffer byteData = new ByteDataBuffer(1048576);

        int byteDataSize = VarInt.readVInt(is);

        for(int i=0;i<byteDataSize;i++) {
            byteData.write((byte)is.read());
        }

        FreeOrdinalTracker freeOrdinalTracker = FreeOrdinalTracker.deserializeFrom(is);

        ByteArrayOrdinalMap deserializedMap = new ByteArrayOrdinalMap(keys, byteData, freeOrdinalTracker, hashedKeyArraySize);

        if(wasPreparedForWrite)
            deserializedMap.prepareForWrite();

        return deserializedMap;
    }

}