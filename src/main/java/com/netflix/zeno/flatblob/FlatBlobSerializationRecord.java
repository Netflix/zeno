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
package com.netflix.zeno.flatblob;

import com.netflix.zeno.fastblob.FastBlobFrameworkSerializer;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFSerializationRecord;

public class FlatBlobSerializationRecord extends NFSerializationRecord {

    private final ByteDataBuffer fieldData[];
    private final boolean isNonNull[];

    /**
     * Create a new FlatBlobSerializationRecord which conforms to the given FastBlobSchema.
     */
    public FlatBlobSerializationRecord(FastBlobSchema schema) {
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
     * This is used by the FlatBlobFrameworkSerializer when writing the data for a specific field.
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
     * verbatim serialized representation in the FlatBlob.
     *
     * @param buf
     */
    public void writeDataTo(ByteDataBuffer buf) {
        for (int i = 0; i < fieldData.length; i++) {
            FieldType fieldType = getSchema().getFieldType(i);
            if (isNonNull[i]) {
                if (fieldType.startsWithVarIntEncodedLength()) {
                    VarInt.writeVInt(buf, (int)fieldData[i].length());
                }
                fieldData[i].copyTo(buf);
            } else {
                if(fieldType == FieldType.FLOAT) {
                    FastBlobFrameworkSerializer.writeNullFloat(buf);
                } else if(fieldType == FieldType.DOUBLE) {
                    FastBlobFrameworkSerializer.writeNullDouble(buf);
                } else {
                    VarInt.writeVNull(buf);
                }
            }
        }
    }

    /**
     * Returns the number of bytes which will be written when writeDataTo(ByteDataBuffer buf) is called.
     *
     * @param buf
     */
    public int sizeOfData() {
        int dataSize = 0;

        for (int i = 0; i < fieldData.length; i++) {
            FieldType fieldType = getSchema().getFieldType(i);
            if (isNonNull[i]) {
                if (fieldType.startsWithVarIntEncodedLength()) {
                    dataSize += VarInt.sizeOfVInt((int)fieldData[i].length());
                }

                dataSize += fieldData[i].length();
            } else {
                if(fieldType == FieldType.FLOAT) {
                    dataSize += 4;
                } else if(fieldType == FieldType.DOUBLE) {
                    dataSize += 8;
                } else {
                    dataSize ++;
                }
            }
        }

        return dataSize;
    }

    /**
     * Reset the ByteDataBuffers for each field.
     */
    public void reset() {
        for (int i = 0; i < fieldData.length; i++) {
            isNonNull[i] = false;
        }
    }
}
