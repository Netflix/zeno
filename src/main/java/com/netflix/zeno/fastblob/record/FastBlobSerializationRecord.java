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
package com.netflix.zeno.fastblob.record;

import com.netflix.zeno.fastblob.FastBlobFrameworkSerializer;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.state.FastBlobTypeSerializationState;
import com.netflix.zeno.serializer.NFSerializationRecord;

/**
 * An NFSerializationRecord for the FastBlobStateEngine serialization framework.<p/>
 *
 * This is the write record for the FastBlob.  It conforms to a FastBlobSchema.<p/>
 *
 * Each field in the schema is assigned a ByteDataBuffer to which the FastBlobFrameworkSerializer will
 * write the bytes for the serialized representation for that field.<p/>
 *
 * Once all of the fields for the object are written, the fields can be concatenated via the writeDataTo() method
 * to some other ByteDataBuffer (in the normal server setup, this will be the ByteDataBuffer in the ByteArrayOrdinalMap).<p/>
 *
 * This class also retains the image membership information.  When an object is added to the FastBlobStateEngine, it
 * is specified which images it should be added to with a boolean array (see {@link FastBlobTypeSerializationState}.add()).
 * This information needs to be propagated down, during traversal for serialization, to each child object which is referenced
 * by the top level object.  A handle to this image membership information is also retained in this record for this purpose.
 *
 * @author dkoszewnik
 *
 */
public class FastBlobSerializationRecord extends NFSerializationRecord {

    private final ByteDataBuffer fieldData[];
    private final boolean isNonNull[];

    private long imageMembershipsFlags;

    /**
     * Create a new FastBlobSerializationRecord which conforms to the given FastBlobSchema.
     */
    public FastBlobSerializationRecord(FastBlobSchema schema) {
        this.fieldData = new ByteDataBuffer[schema.numFields()];
        this.isNonNull = new boolean[schema.numFields()];
        for (int i = 0; i < fieldData.length; i++) {
            fieldData[i] = new ByteDataBuffer(32);
        }
        setSchema(schema);
    }

    /**
     * Returns the buffer which should be used to serialize the data for the given field.
     *
     * @param field
     * @return
     */
    public ByteDataBuffer getFieldBuffer(String field) {
        int fieldPosition = getSchema().getPosition(field);
        return getFieldBuffer(fieldPosition);
    }

    /**
     * Returns the buffer which should be used to serialize the data for the field at the given position in the schema.<p/>
     *
     * This is used by the FastBlobFrameworkSerializer when writing the data for a specific field.
     *
     * @param field
     * @return
     */
    public ByteDataBuffer getFieldBuffer(int fieldPosition) {
        isNonNull[fieldPosition] = true;
        fieldData[fieldPosition].reset();
        return fieldData[fieldPosition];
    }

    /**
     * Concatenates all fields, in order, to the ByteDataBuffer supplied.  This concatenation is the
     * verbatim serialized representation in the FastBlob.
     *
     * @param buf
     */
    public void writeDataTo(ByteDataBuffer buf) {
        for (int i = 0; i < fieldData.length; i++) {
            if (isNonNull[i]) {
                if (getSchema().getFieldType(i).startsWithVarIntEncodedLength())
                    VarInt.writeVInt(buf, (int)fieldData[i].length());
                fieldData[i].copyTo(buf);
            } else {
                if(getSchema().getFieldType(i) == FieldType.FLOAT) {
                    FastBlobFrameworkSerializer.writeNullFloat(buf);
                } else if(getSchema().getFieldType(i) == FieldType.DOUBLE) {
                    FastBlobFrameworkSerializer.writeNullDouble(buf);
                } else {
                    VarInt.writeVNull(buf);
                }
            }
        }
    }

    /**
     * Reset the ByteDataBuffers for each field.
     */
    public void reset() {
        for (int i = 0; i < fieldData.length; i++) {
            isNonNull[i] = false;
        }
    }

    /**
     * This is the image membership information for the object represented by this record.<p/>
     *
     * It is contained here so that it may be passed down by the FastBlobFrameworkSerializer when
     * making the call to serialize child objects which are referenced by this object.
     *
     * @param imageMembershipsFlags
     */
    public void setImageMembershipsFlags(long imageMembershipsFlags) {
        this.imageMembershipsFlags = imageMembershipsFlags;
    }

    public long getImageMembershipsFlags() {
        return imageMembershipsFlags;
    }

}
