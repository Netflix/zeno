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

import com.netflix.zeno.fastblob.FastBlobHeapFriendlyClientFrameworkSerializer;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.StreamingByteData;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.state.ByteArrayOrdinalMap;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.serializer.NFTypeSerializer;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads FastBlob snapshots and deltas from streams.<p/>
 *
 * The modifications will be applied to the FastBlobStateEngine supplied in the constructor.
 */
public class FastBlobReader {

    private final FastBlobStateEngine stateEngine;

    private FastBlobHeaderReader headerReader;
    private FastBlobReaderEventHandler eventHandler = null;

    public FastBlobReader(FastBlobStateEngine stateEngine) {
        this.stateEngine = stateEngine;
        this.headerReader = new ZenoFastBlobHeaderReader();
    }

    public void setFastBlobHeaderReader(FastBlobHeaderReader headerReader) {
        this.headerReader = headerReader;
    }

    public void setEventHandler(FastBlobReaderEventHandler eventHandler) {
        this.eventHandler = eventHandler;
    }

    /**
     * Read a snapshot from the specified stream.  Apply the snapshot to the FastBlobStateEngine supplied in the constructor of this class.
     */
    public void readSnapshot(InputStream is) throws IOException {
        FastBlobHeader header = readHeader(is);

        StreamingByteData byteData = getStreamingByteData(is, header.getDeserializationBufferSizeHint());
        DataInputStream dis = new DataInputStream(byteData);

        int numTypes = header.getNumberOfTypes();

        if(stateEngine.getLatestVersion() == null) {
            readSnapshotTypes(byteData, dis, numTypes);
        } else {
            readSnapshotTypesDoubleSnapshotRefresh(byteData, dis, numTypes);
        }

        ///The version must be set *after* the changes are applied.  This will protect against
        ///bad data in the event of an Exception midway through parsing.
        stateEngine.setLatestVersion(header.getVersion());
    }

    /**
     * Read a snapshot with no current states populated.
     */
    private void readSnapshotTypes(StreamingByteData byteData, DataInputStream dis, int numTypes) throws IOException {
        for(int i=0;i<numTypes;i++) {
            /// type flags byte -- reserved for later use
            dis.read();

            FastBlobSchema schema = FastBlobSchema.readFrom(dis);

            readTypeStateObjects(byteData, schema);
        }
    }

    /**
     * Read a snapshot with a state currently populated.  This is the "heap-friendly" version
     */
    private void readSnapshotTypesDoubleSnapshotRefresh(StreamingByteData byteData, DataInputStream dis, int numTypes) throws IOException {
        ByteArrayOrdinalMap serializedRepresentationMap = new ByteArrayOrdinalMap();
        stateEngine.prepareForDoubleSnapshotRefresh();

        for(int i=0;i<numTypes;i++) {
            /// type flags byte -- reserved for later use
            dis.read();

            FastBlobSchema schema = FastBlobSchema.readFrom(dis);

            readTypeStateObjectsDoubleSnapshotRefresh(byteData, schema, serializedRepresentationMap);

            serializedRepresentationMap.clear();
        }

        stateEngine.cleanUpAfterDoubleSnapshotRefresh();
    }

    /**
     * Read a delta from the specified stream.  Apply the delta to the FastBlobStateEngine supplied in the constructor of this class.<p/>
     *
     * This method performs no validation of the data.  It is assumed that the data currently represented in the FastBlobStateEngine is
     * in the state which the server was in when it was produced this delta.  Otherwise, the results are undefined.
     */
    public void readDelta(InputStream is) throws IOException {
        FastBlobHeader header = readHeader(is);

        StreamingByteData byteData = getStreamingByteData(is, header.getDeserializationBufferSizeHint());
        DataInputStream dis = new DataInputStream(byteData);

        int numTypes = header.getNumberOfTypes();

        for(int i=0;i<numTypes;i++) {
            /// type flags byte -- reserved for later use
            dis.read();

            FastBlobSchema schema = FastBlobSchema.readFrom(dis);

            readTypeStateRemovals(byteData, schema);
            readTypeStateObjects(byteData, schema);
        }

        ///The version must be set *after* the changes are applied.  This will protect against
        ///bad data in the event of an Exception midway through parsing.
        stateEngine.setLatestVersion(header.getVersion());
    }

    /**
     * Read the header and return the version
     */
    private FastBlobHeader readHeader(InputStream is) throws IOException {
        FastBlobHeader header = headerReader.readHeader(is);
        stateEngine.addHeaderTags(header.getHeaderTags());
        return header;
    }

    private StreamingByteData getStreamingByteData(InputStream is, int deserializationBufferSizeHint) throws IOException {
        StreamingByteData byteData = new StreamingByteData(is, deserializationBufferSizeHint);
        return byteData;
    }

    private void readTypeStateRemovals(StreamingByteData byteData, FastBlobSchema schema) throws IOException {
        FastBlobTypeDeserializationState<?> typeDeserializationState = stateEngine.getTypeDeserializationState(schema.getName());

        int numRemovals = VarInt.readVInt(byteData);
        int currentRemoval = 0;

        if(numRemovals != 0 && eventHandler != null) {
            eventHandler.removedObjects(schema.getName(), numRemovals);
        }

        for(int i=0;i<numRemovals;i++) {
            currentRemoval += VarInt.readVInt(byteData);
            if(typeDeserializationState != null) {
                typeDeserializationState.remove(currentRemoval);
            }
        }
    }

    private void readTypeStateObjects(StreamingByteData byteData,FastBlobSchema schema) throws IOException {
        FastBlobDeserializationRecord rec = new FastBlobDeserializationRecord(schema, byteData);
        FastBlobTypeDeserializationState<?> typeDeserializationState = stateEngine.getTypeDeserializationState(schema.getName());

        int numObjects = VarInt.readVInt(byteData);

        if(numObjects != 0 && eventHandler != null) {
            eventHandler.addedObjects(schema.getName(), numObjects);
        }

        int currentOrdinal = 0;

        for(int j=0;j<numObjects;j++) {
            int currentOrdinalDelta = VarInt.readVInt(byteData);

            currentOrdinal += currentOrdinalDelta;

            int objectSize = rec.position(byteData.currentStreamPosition());
            byteData.incrementStreamPosition(objectSize);

            if(typeDeserializationState != null) {
                typeDeserializationState.add(currentOrdinal, rec);
            }
        }
    }

    private <T> void readTypeStateObjectsDoubleSnapshotRefresh(StreamingByteData byteData, FastBlobSchema schema, ByteArrayOrdinalMap map) throws IOException{
        FastBlobHeapFriendlyClientFrameworkSerializer frameworkSerializer = (FastBlobHeapFriendlyClientFrameworkSerializer)stateEngine.getFrameworkSerializer();
        FastBlobDeserializationRecord rec = new FastBlobDeserializationRecord(schema, byteData);
        FastBlobTypeDeserializationState<T> typeDeserializationState = stateEngine.getTypeDeserializationState(schema.getName());
        FastBlobSerializationRecord serializationRecord = null;
        ByteDataBuffer deserializedRecordBuffer = null;

        int numObjects = VarInt.readVInt(byteData);
        int numObjectsReused = 0;
        int numFlawedSerializationIntegrity = 0;

        if(numObjects != 0 && eventHandler != null) {
            eventHandler.addedObjects(schema.getName(), numObjects);
        }

        if(typeDeserializationState != null) {
            serializationRecord = new FastBlobSerializationRecord(typeDeserializationState.getSchema());

            frameworkSerializer.setCheckSerializationIntegrity(false);

            deserializedRecordBuffer = new ByteDataBuffer();
            typeDeserializationState.populateByteArrayOrdinalMap(map);

            frameworkSerializer.setCheckSerializationIntegrity(true);
        }

        int currentOrdinal = 0;

        for(int j=0;j<numObjects;j++) {
            int currentOrdinalDelta = VarInt.readVInt(byteData);

            currentOrdinal += currentOrdinalDelta;

            int recordSize = rec.position(byteData.currentStreamPosition());

            if(typeDeserializationState != null) {
                NFTypeSerializer<T> serializer = typeDeserializationState.getSerializer();
                T deserializedObject = serializer.deserialize(rec);
                serializer.serialize(deserializedObject, serializationRecord);
                serializationRecord.writeDataTo(deserializedRecordBuffer);

                int previousOrdinal = map.get(deserializedRecordBuffer);

                serializationRecord.reset();
                deserializedRecordBuffer.reset();

                if(previousOrdinal != -1 && !frameworkSerializer.isSerializationIntegrityFlawed()) {
                    typeDeserializationState.copyPrevious(currentOrdinal, previousOrdinal);
                    numObjectsReused++;
                } else {
                    if(frameworkSerializer.isSerializationIntegrityFlawed()) {
                        numFlawedSerializationIntegrity++;
                    }
                    typeDeserializationState.add(currentOrdinal, rec);
                }

                frameworkSerializer.clearSerializationIntegrityFlawedFlag();
            }

            byteData.incrementStreamPosition(recordSize);
        }

        if(typeDeserializationState != null) {
            typeDeserializationState.clearPreviousObjects();
            typeDeserializationState.createIdentityOrdinalMap();
        }

        if(eventHandler != null) {
            if(numObjects != 0) {
                eventHandler.reusedObjects(schema.getName(), numObjectsReused);
            }
            if(numFlawedSerializationIntegrity != 0) {
                eventHandler.objectsFailedReserialization(schema.getName(), numFlawedSerializationIntegrity);
            }
        }
    }

}
