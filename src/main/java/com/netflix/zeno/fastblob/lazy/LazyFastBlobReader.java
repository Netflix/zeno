/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.zeno.fastblob.lazy;

import com.netflix.zeno.fastblob.io.FastBlobHeader;
import com.netflix.zeno.fastblob.io.FastBlobHeaderReader;
import com.netflix.zeno.fastblob.io.FastBlobReaderEventHandler;
import com.netflix.zeno.fastblob.io.ZenoFastBlobHeaderReader;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.StreamingByteData;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads FastBlob snapshots and deltas from streams.<p/>
 *
 * The modifications will be applied to the FastBlobStateEngine supplied in the constructor.
 */
public class LazyFastBlobReader {

    private final LazyStateEngine stateEngine;

    private FastBlobHeaderReader headerReader;
    private FastBlobReaderEventHandler eventHandler = null;

    public LazyFastBlobReader(LazyStateEngine stateEngine) {
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

            System.out.println(schema.getName());

            readTypeStateObjects(byteData, schema);
        }
    }

    /**
     * Read the header and return the version
     */
    private FastBlobHeader readHeader(InputStream is) throws IOException {
        FastBlobHeader header = headerReader.readHeader(is, stateEngine);
        stateEngine.addHeaderTags(header.getHeaderTags());
        return header;
    }

    private StreamingByteData getStreamingByteData(InputStream is, int deserializationBufferSizeHint) throws IOException {
        StreamingByteData byteData = new StreamingByteData(is, deserializationBufferSizeHint);
        return byteData;
    }

    private void readTypeStateObjects(StreamingByteData byteData,FastBlobSchema schema) throws IOException {
        FastBlobDeserializationRecord rec = new FastBlobDeserializationRecord(schema, byteData);
        LazyTypeDeserializationState<?> typeDeserializationState = stateEngine.getTypeDeserializationState(schema.getName());

        int numObjects = VarInt.readVInt(byteData);

        if(numObjects != 0 && eventHandler != null) {
            eventHandler.addedObjects(schema.getName(), numObjects);
        }

        int currentOrdinal = 0;

        for(int j=0;j<numObjects;j++) {
            int currentOrdinalDelta = VarInt.readVInt(byteData);

            currentOrdinal += currentOrdinalDelta;

            int objectSize = rec.position(byteData.currentStreamPosition());

            if(typeDeserializationState != null) {
                typeDeserializationState.add(currentOrdinal, byteData, byteData.currentStreamPosition(), objectSize);
            }

            byteData.incrementStreamPosition(objectSize);
        }

        if(typeDeserializationState != null) {
            typeDeserializationState.setFastBlobSchema(schema);
            typeDeserializationState.finalizePointers();
        }
    }

}
