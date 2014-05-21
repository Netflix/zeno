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
package com.netflix.zeno.fastblob.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.fastblob.state.FastBlobTypeSerializationState;
import com.netflix.zeno.fastblob.state.ThreadSafeBitSet;

/**
 * Writes FastBlob images to streams.
 */
public class FastBlobWriter {

    private final FastBlobStateEngine stateEngine;
    private final int imageIndex;
    private FastBlobHeaderWriter headerWriter;

    public FastBlobWriter(FastBlobStateEngine stateEngine) {
        this(stateEngine, 0);
    }

    public FastBlobWriter(FastBlobStateEngine stateEngine, int imageIndex) {
        this(stateEngine, imageIndex, new ZenoFastBlobHeaderWriter());
    }

    public void setFastBlobHeaderWriter(FastBlobHeaderWriter headerWriter) {
        this.headerWriter = headerWriter;
    }

    /**
     * This FastBlobWriter will write a single image to a stream, as either a snapshot or delta.<p/>
     *
     * The configuration for the image which this will write is contained in the list returned by <code>stateEngine.getImageConfigurations()</code>
     * at the index specified by <code>imageIndex</code>
     *
     * @param stateEngine
     * @param imageIndex
     */
    public FastBlobWriter(FastBlobStateEngine stateEngine, int imageIndex, FastBlobHeaderWriter headerWriter) {
        this.stateEngine = stateEngine;
        this.imageIndex = imageIndex;
        this.headerWriter = headerWriter;
    }

    /**
     * Write a snapshot to the specified stream.
     */
    public void writeSnapshot(OutputStream os) throws Exception {
        writeSnapshot(new DataOutputStream(os));
    }

    public void writeSnapshot(DataOutputStream os) throws IOException {
        writeHeader(os);

        for(FastBlobTypeSerializationState<?> typeState : stateEngine.getOrderedSerializationStates()) {
            if(!typeState.isReadyForWriting())
                throw new RuntimeException("This state engine is not ready for writing! Have you remembered to call stateEngine.prepareForWrite()?");

            /// type flags byte -- reserved for later use
            os.write(0);
            /// write the schema
            typeState.getSchema().writeTo(os);

            ThreadSafeBitSet imageMembershipBitSet = typeState.getImageMembershipBitSet(imageIndex);
            serializeTypeStateObjects(os, typeState, imageMembershipBitSet);
        }
    }

    public void writeNonImageSpecificSnapshot(DataOutputStream os) throws IOException {
        writeHeader(os);

        for(FastBlobTypeSerializationState<?> typeState : stateEngine.getOrderedSerializationStates()) {
            if(!typeState.isReadyForWriting())
                throw new RuntimeException("This state engine is not ready for writing! Have you remembered to call stateEngine.prepareForWrite()?");

            FastBlobTypeDeserializationState<?> typeDeserializationState = stateEngine.getTypeDeserializationState(typeState.getSchema().getName());

            /// type flags byte -- reserved for later use
            os.write(0);
            /// write the schema
            typeState.getSchema().writeTo(os);

            serializeTypeStateObjects(os, typeState, typeDeserializationState);
        }
    }

    /**
     * Write a delta to the specified stream.
     */
    public void writeDelta(OutputStream os) throws IOException {
        writeDelta(new DataOutputStream(os));
    }

    public void writeDelta(DataOutputStream os) throws IOException {
        writeHeader(os);

        for(FastBlobTypeSerializationState<?> typeState : stateEngine.getOrderedSerializationStates()) {
            if(!typeState.isReadyForWriting())
                throw new RuntimeException("This state engine is not ready for writing! Have you remembered to call stateEngine.prepareForWrite()?");

            /// type flags byte -- reserved for later use
            os.write(0);
            /// write the schema
            typeState.getSchema().writeTo(os);

            ThreadSafeBitSet currentImageMembershipBitSet = typeState.getImageMembershipBitSet(imageIndex);
            ThreadSafeBitSet previousImageMembershipBitSet = typeState.getPreviousCycleImageMembershipBitSet(imageIndex);

            serializeDelta(os, typeState, currentImageMembershipBitSet, previousImageMembershipBitSet);
        }
    }

    /**
     * Write a reverse delta to the specified stream.
     *
     * A reverse delta is the opposite of a delta.  A delta removes all unused objects from the previous state and adds all
     * new objects in the current state.  A reverse delta removes all new objects in the current state and adds all unused
     * objects from the previous state.
     */
    public void writeReverseDelta(OutputStream os, String previousVersion) throws IOException {
        writeReverseDelta(new DataOutputStream(os), previousVersion);
    }

    public void writeReverseDelta(DataOutputStream os, String previousVersion) throws IOException {
        writeHeader(os, previousVersion);

        for(FastBlobTypeSerializationState<?> typeState : stateEngine.getOrderedSerializationStates()) {
            if(!typeState.isReadyForWriting())
                throw new RuntimeException("This state engine is not ready for writing! Have you remembered to call stateEngine.prepareForWrite()?");

            if(typeState.getPreviousStateSchema() != null) {
                /// type flags byte -- reserved for later use
                os.write(0);
                /// write the schema
                typeState.getPreviousStateSchema().writeTo(os);

                ThreadSafeBitSet currentImageMembershipBitSet = typeState.getImageMembershipBitSet(imageIndex);
                ThreadSafeBitSet previousImageMembershipBitSet = typeState.getPreviousCycleImageMembershipBitSet(imageIndex);

                serializeDelta(os, typeState, previousImageMembershipBitSet, currentImageMembershipBitSet);
            }
        }
    }

    private void serializeDelta(DataOutputStream os, FastBlobTypeSerializationState<?> typeState, ThreadSafeBitSet currentStateOrdinals, ThreadSafeBitSet prevStateOrdinals) throws IOException {
        /// get all of the ordinals contained in the previous cycle, which are no longer contained in this cycle.  These all need to be removed.
        ThreadSafeBitSet removedTypeStateObjectsBitSet = prevStateOrdinals.andNot(currentStateOrdinals);
        serializeTypeStateRemovals(os, removedTypeStateObjectsBitSet);

        /// get all of the ordinals contained in this cycle, which were not contained in the previous cycle.  These all need to be added.
        ThreadSafeBitSet addedTypeStateObjectsBitSet = currentStateOrdinals.andNot(prevStateOrdinals);
        serializeTypeStateObjects(os, typeState, addedTypeStateObjectsBitSet);
    }

    private void writeHeader(DataOutputStream os) throws IOException {
        String version = stateEngine.getLatestVersion() != null ? stateEngine.getLatestVersion() : "";
        writeHeader(os, version);
    }

    private void writeHeader(DataOutputStream os, String version) throws IOException {
        FastBlobHeader header = new FastBlobHeader();
        header.setVersion(version);
        header.setHeaderTags(stateEngine.getHeaderTags());

        /// The deserialization StreamingByteData buffer size needs to accommodate the largest single object.
        /// write the ceil(log2(maxSize)) as a single byte at the beginning of the stream.
        /// upon deserialization, this byte will be read and the StreamingByteData buffer can be sized appropriately.
        int deserializationBufferSizeHint = 32 - Integer.numberOfLeadingZeros(stateEngine.getMaxSingleObjectLength() - 1);
        header.setDeserializationBufferSizeHint(deserializationBufferSizeHint);

        header.setNumberOfTypes(stateEngine.getOrderedSerializationStates().size());

        headerWriter.writeHeader(header,stateEngine,os);
    }

    private void serializeTypeStateObjects(DataOutputStream os, FastBlobTypeSerializationState<?> typeState, ThreadSafeBitSet includeOrdinals) throws IOException {
        int currentBitSetCapacity = includeOrdinals.currentCapacity();
        int currentOrdinal = 0;

        /// write the number of objects
        VarInt.writeVInt(os, includeOrdinals.cardinality());

        for(int i=0;i<currentBitSetCapacity;i++) {
            if(includeOrdinals.get(i)) {
                /// gap-encoded ordinals
                VarInt.writeVInt(os, i - currentOrdinal);
                currentOrdinal = i;

                /// typeState will use the ByteArrayOrdinalMap to write the length and
                /// serialized representation of the object.
                typeState.writeObjectTo(os, i);
            }
        }
    }

    private void serializeTypeStateObjects(DataOutputStream os, FastBlobTypeSerializationState<?> typeState,
            FastBlobTypeDeserializationState<?> typeDeserializationState) throws IOException {
        /// write the number of objects
        VarInt.writeVInt(os, typeDeserializationState.countObjects());
        int currentOrdinal = 0;

        for(int i=0;i<=typeDeserializationState.maxOrdinal();i++) {
            Object obj = typeDeserializationState.get(i);
            if(obj != null) {
                /// gap-encoded ordinals
                VarInt.writeVInt(os, i - currentOrdinal);
                currentOrdinal = i;

                /// typeState will use the ByteArrayOrdinalMap to write the length and
                /// serialized representation of the object.
                typeState.writeObjectTo(os, i);
            }
        }
    }

    private void serializeTypeStateRemovals(DataOutputStream os, ThreadSafeBitSet removals) throws IOException {
        int bitSetCapacity = removals.currentCapacity();
        int currentRemoval = 0;

        /// write the number of removals
        VarInt.writeVInt(os, removals.cardinality());

        for(int i=0;i<bitSetCapacity;i++) {
            if(removals.get(i)) {
                /// gap-encoded ordinals
                VarInt.writeVInt(os, i - currentRemoval);
                currentRemoval = i;
            }
        }
    }

}
