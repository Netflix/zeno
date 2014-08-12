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

import com.netflix.zeno.fastblob.FastBlobImageUtils;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.OrdinalMapping;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.state.WeakObjectOrdinalMap.Entry;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class represents the "serialization state" for a single type at some level of the object
 * hierarchy in the serialized data.<p/>
 *
 * This class is responsible for maintaining the mappings between serialized representations of
 * its type and ordinals.  It performs this responsibility by using a {@link ByteArrayOrdinalMap}.<p/>
 *
 * This class is also responsible for maintaining data about the set of objects<p/>
 *
 * This class has a lifecycle during which it alternates between two states:<p/>
 *
 * <ol>
 * <li>Safe to add objects, but not safe to write contained objects to a stream.</li>
 * <li>Not safe to add objects, but safe to write contained objects to a stream.</li>
 * </ol><p/>
 *
 * Initially the object will be in state (1).<br/>
 * From state (1), if prepareForWrite() is called, it will be transitioned to state (2).<br/>
 * From state (2), calling prepareForNextCycle() will transition back to state (1).<p/>
 *
 * It is safe for multiple threads to add to this state engine or write from this state engine.  It<br/>
 * is not safe for multiple threads to make lifecycle transitions (all threads must agree on a single state).
 *
 * @author dkoszewnik
 *
 */
public class FastBlobTypeSerializationState<T> {

    public final NFTypeSerializer<T> serializer;
    private FastBlobSchema typeSchema;
    private FastBlobSchema previousStateTypeSchema;

    private final ThreadLocal<FastBlobSerializationRecord> serializationRecord;
    private final ThreadLocal<ByteDataBuffer> serializedScratchSpace;

    private ByteArrayOrdinalMap ordinalMap;

    private ThreadSafeBitSet imageMemberships[];
    private ThreadSafeBitSet previousCycleImageMemberships[];

    private WeakObjectOrdinalMap objectOrdinalMap;

    /**
     *
     * @param serializer
     *            The NFTypeSerializer for this state's type.
     * @param numImages
     *            The number of blob images which will be produced by the
     *            {@link FastBlobStateEngine}.
     */
    public FastBlobTypeSerializationState(NFTypeSerializer<T> serializer, int numImages) {
        this(serializer, numImages, true);
    }

    /**
     *
     * @param serializer The NFTypeSerializer for this state's type.
     * @param numImages The number of blob images which will be produced by the {@link FastBlobStateEngine}.
     */
    public FastBlobTypeSerializationState(NFTypeSerializer<T> serializer, int numImages, boolean shouldUseObjectIdentityOrdinalCaching) {
        this.serializer = serializer;
        this.typeSchema = serializer.getFastBlobSchema();
        this.serializationRecord = new ThreadLocal<FastBlobSerializationRecord>();
        this.serializedScratchSpace = new ThreadLocal<ByteDataBuffer>();
        this.ordinalMap = new ByteArrayOrdinalMap();

        this.imageMemberships = initializeImageMembershipBitSets(numImages);
        this.previousCycleImageMemberships = initializeImageMembershipBitSets(numImages);
        if (shouldUseObjectIdentityOrdinalCaching) {
            objectOrdinalMap = new WeakObjectOrdinalMap(8);
        }
    }

    public String getName() {
        return serializer.getName();
    }

    public FastBlobSchema getSchema() {
        return typeSchema;
    }

    /**
     * This is only useful when we start a new server with a different schema than the previous server.
     * The previous state schema gets loaded from the previously serialized previousStateTypeSchemaserver state.
     *
     */
    public FastBlobSchema getPreviousStateSchema() {
        return previousStateTypeSchema;
    }

    /**
     * Add an object to this state. We will create a serialized representation
     * of this object, then assign or retrieve the ordinal for this serialized
     * representation in our {@link ByteArrayOrdinalMap}
     * <p/>
     *
     * Because the FastBlobStateEngine can represent multiple images, it must be
     * specified in *which* images this object should be included. This is
     * accomplished with a boolean array. If the object is included in a
     * specific image, then the imageMembershipsFlag array will contain the
     * boolean value "true", at the index in which that image appears in the
     * list returned by {@link FastBlobStateEngine}.getImageConfigurations()
     *
     * @param data
     * @param imageMembershipsFlags
     */
    @Deprecated
    public int add(T data, boolean[] imageMembershipsFlags) {
        return add(data, FastBlobImageUtils.toLong(imageMembershipsFlags));
    }

    /**
     * Add an object to this state.  We will create a serialized representation of this object, then
     * assign or retrieve the ordinal for this serialized representation in our {@link ByteArrayOrdinalMap}<p/>
     *
     * Because the FastBlobStateEngine can represent multiple images, it must be specified in *which* images
     * this object should be included.  This is accomplished with a boolean array.  If the object is included
     * in a specific image, then the imageMembershipsFlag array will contain the boolean value "true", at the index
     * in which that image appears in the list returned by {@link FastBlobStateEngine}.getImageConfigurations()
     *
     * @param data
     * @param imageMembershipsFlags
     */
    public int add(T data, long imageMembershipsFlags) {
        if(!ordinalMap.isReadyForAddingObjects())
            throw new RuntimeException("The FastBlobStateEngine is not ready to add more Objects.  Did you remember to call stateEngine.prepareForNextCycle()?");

        if (objectOrdinalMap != null) {
            Entry existingEntry = objectOrdinalMap.getEntry(data);
            if (existingEntry != null) {
                if (existingEntry.hasImageMembershipsFlags(imageMembershipsFlags)) {
                    return existingEntry.getOrdinal();
                }
            }
        }

        FastBlobSerializationRecord rec = record();

        rec.setImageMembershipsFlags(imageMembershipsFlags);

        serializer.serialize(data, rec);

        ByteDataBuffer scratch = scratch();
        rec.writeDataTo(scratch);

        int ordinal = addData(scratch, imageMembershipsFlags);

        scratch.reset();
        rec.reset();

        if (objectOrdinalMap != null) {
            objectOrdinalMap.put(data, ordinal, imageMembershipsFlags);
        }
        return ordinal;
    }

    /**
     * Hook to add raw data. This is used during FastBlobStateEngine
     * combination. PreviousState
     *
     * @param data
     * @param imageMembershipsFlags
     * @return
     */
    @Deprecated
    public int addData(ByteDataBuffer data, boolean[] imageMembershipsFlags) {
        return addData(data, FastBlobImageUtils.toLong(imageMembershipsFlags));
    }

    /**
     * Hook to add raw data.  This is used during FastBlobStateEngine combination.
     *PreviousState
     * @param data
     * @param imageMembershipsFlags
     * @return
     */
    public int addData(ByteDataBuffer data, long imageMembershipsFlags) {
        int ordinal = ordinalMap.getOrAssignOrdinal(data);

        addOrdinalToImages(imageMembershipsFlags, ordinal);

        return ordinal;
    }

    /**
     * Copy the state data into the provided FastBlobTypeSerializationState.<p/>
     *
     * This is used during FastBlobStateEngine combination.<p/>
     *
     * Thread safety:  This cannot be safely called concurrently with add() operations to *this* state engine.<p/>
     *
     * @param otherState
     * @param stateOrdinalMappers
     */
    public void copyTo(FastBlobTypeSerializationState<?> otherState, OrdinalMapping ordinalMapping) {
        ordinalMap.copySerializedObjectData(otherState, imageMemberships, ordinalMapping);
    }


    /**
     * Fill the data from this serialization state into the provided FastBlobTypeDeserializationState<p/>
     *
     * The provided deserialization state should be of the exact same type as this FastBlobTypeSerializationState (it should contain
     * exactly the same schema).<p/>
     *
     * @param otherState
     */
    public void fillDeserializationState(FastBlobTypeDeserializationState<?> otherState) {
       otherState.populateFromByteOrdinalMap(ordinalMap);
    }

    /**
     * Called to perform a state transition.<p/>
     *
     * Precondition: We are adding objects to this state engine.<br/>
     * Postcondition: We are writing the previously added objects to a FastBlob.
     *
     * @return the length of the maximum serialized object representation for this type.
     */
    public int prepareForWrite() {
        int maxLengthOfAnyRecord = ordinalMap.prepareForWrite();

        return maxLengthOfAnyRecord;
    }

    /**
     * Called to perform a state transition.<p/>
     *
     * Precondition: We are writing the previously added objects to a FastBlob.<br/>
     * Postcondition: We are ready to add objects to this state engine for the next server cycle.
     */
    public void prepareForNextCycle() {
        ThreadSafeBitSet usedOrdinals = ThreadSafeBitSet.orAll(imageMemberships);

        ordinalMap.compact(usedOrdinals);

        ThreadSafeBitSet temp[] = previousCycleImageMemberships;
        previousCycleImageMemberships = imageMemberships;
        imageMemberships = temp;

        previousStateTypeSchema = typeSchema;
        typeSchema = serializer.getFastBlobSchema();

        for(ThreadSafeBitSet bitSet : imageMemberships) {
            bitSet.clearAll();
        }
        if (objectOrdinalMap != null) {
            objectOrdinalMap.clear();
        }
    }

    /**
     * Write the serialized representation of the object assigned to the specified ordinal to the stream.
     */
    public void writeObjectTo(OutputStream os, int ordinal) throws IOException {
        ordinalMap.writeSerializedObject(os, ordinal);
    }

    /**
     * Is this type state engine in the cycle stage which allows for writing of blob data?
     */
    public boolean isReadyForWriting() {
        return ordinalMap.isReadyForWriting();
    }

    /**
     * @param imageIndex the index of an image in the list returned by FastBlobStateEngine.getImageConfigurations()
     *
     * @return the bit set specifying which ordinals were referenced in the image at the given index during the current cycle.
     */
    public ThreadSafeBitSet getImageMembershipBitSet(int imageIndex) {
        return imageMemberships[imageIndex];
    }

    /**
     * @param imageIndex the index of an image in the list returned by FastBlobStateEngine.getImageConfigurations()
     *
     * @return the bit set specifying which ordinals were referenced in the image at the given index during the previous cycle.
     */
    public ThreadSafeBitSet getPreviousCycleImageMembershipBitSet(int imageIndex) {
        return previousCycleImageMemberships[imageIndex];
    }

    /**
     * Update the bit sets for image membership to indicate that the specified
     * ordinal was referenced.
     *
     * @see com.netflix.zeno.fastblob.FastBlobImageUtils.toInteger
     *
     * @param imageMembershipsFlags
     * @param ordinal
     */
    private void addOrdinalToImages(long imageMembershipsFlags, int ordinal) {
        // This code is tightly related to FastBlobImageUtils packing order
        int count = 0;
        while (imageMembershipsFlags != 0) {
            if ((imageMembershipsFlags & 1) != 0) {
                imageMemberships[count].set(ordinal);
            }
            imageMembershipsFlags = imageMembershipsFlags >>> 1;
            count++;
        }
    }

    private ThreadSafeBitSet[] initializeImageMembershipBitSets(int numImages) {
        ThreadSafeBitSet sets[] = new ThreadSafeBitSet[numImages];

        for(int i=0;i<numImages;i++) {
            sets[i] = new ThreadSafeBitSet();
        }

        return sets;
    }

    /**
     * Get or create a scratch byte array.  Each thread will need its own array, so these
     * are referenced via a ThreadLocal variable.
     */
    private ByteDataBuffer scratch() {
        ByteDataBuffer scratch = serializedScratchSpace.get();
        if(scratch == null) {
            scratch = new ByteDataBuffer(32);
            serializedScratchSpace.set(scratch);
        }
        return scratch;
    }

    /**
     * Get or create a FastBlobSerializationRecord.  Each thread will create and reuse its own record,
     * so these are referenced via a ThreadLocal variable.
     */
    private FastBlobSerializationRecord record() {
        FastBlobSerializationRecord rec = serializationRecord.get();
        if(rec == null) {
            rec = new FastBlobSerializationRecord(typeSchema);
            serializationRecord.set(rec);
        }
        return rec;
    }

    /**
     * Serialize this FastBlobTypeSerializationState to an OutputStream
     */
    public void serializeTo(DataOutputStream os) throws IOException {
        typeSchema.writeTo(os);

        ordinalMap.serializeTo(os);

        for(ThreadSafeBitSet bitSet : imageMemberships) {
            bitSet.serializeTo(os);
        }
    }

    /**
     * Deserialize this FastBlobTypeSerializationState from an InputStream
     */
    public void deserializeFrom(DataInputStream is, int numConfigs) throws IOException {
        typeSchema = FastBlobSchema.readFrom(is);

        ordinalMap = ByteArrayOrdinalMap.deserializeFrom(is);

        for(int i=0;i<numConfigs;i++) {
            ThreadSafeBitSet bitSet = ThreadSafeBitSet.deserializeFrom(is);
            imageMemberships[i] = bitSet;
        }
    }

    /**
     * Discard a serialized state -- this happens if an object type is completely removed.
     */
    public static void discardSerializedTypeSerializationState(DataInputStream is, int numConfigs) throws IOException {
        FastBlobSchema.readFrom(is);
        ByteArrayOrdinalMap.deserializeFrom(is);
        for(int i=0;i<numConfigs;i++)
            ThreadSafeBitSet.deserializeFrom(is);
    }
}
