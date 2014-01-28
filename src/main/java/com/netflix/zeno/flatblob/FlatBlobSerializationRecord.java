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

import static com.netflix.zeno.flatblob.FlatBlobFrameworkSerializer.NULL_DOUBLE_BITS;
import static com.netflix.zeno.flatblob.FlatBlobFrameworkSerializer.NULL_FLOAT_BITS;

import com.netflix.zeno.fastblob.FastBlobFrameworkSerializer;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobSchema;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFSerializationRecord;

import java.io.IOException;
import java.io.OutputStream;

public class FlatBlobSerializationRecord implements NFSerializationRecord {

    private final FastBlobSchema schema;
    private final ByteDataBuffer fieldData[];
    private final boolean isNonNull[];

    /**
     * Create a new FlatBlobSerializationRecord which conforms to the given FastBlobSchema.
     */
    public FlatBlobSerializationRecord(FastBlobSchema schema) {
        this.schema = schema;
        this.fieldData = new ByteDataBuffer[schema.numFields()];
        this.isNonNull = new boolean[schema.numFields()];
        for (int i = 0; i < fieldData.length; i++) {
            fieldData[i] = new ByteDataBuffer(32);
        }
    }

    public FastBlobSchema getSchema() {
        return schema;
    }

    /**
     * Returns the buffer which should be used to serialize the data for the given field.
     *
     * @param field
     * @return
     */
    public ByteDataBuffer getFieldBuffer(String field) {
        int fieldPosition = schema.getPosition(field);
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
            FieldType fieldType = schema.getFieldType(i);
            if (isNonNull[i]) {
                if (fieldType.startsWithVarIntEncodedLength()) {
                    VarInt.writeVInt(buf, fieldData[i].length());
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
     * Concatenates all fields, in order, to the ByteDataBuffer supplied.  This concatenation is the
     * verbatim serialized representation in the FlatBlob.
     *
     * @param buf
     * @throws IOException
     */
    public void writeDataTo(OutputStream os) throws IOException {
        for (int i = 0; i < fieldData.length; i++) {
            FieldType fieldType = schema.getFieldType(i);
            if (isNonNull[i]) {
                int length = fieldData[i].length();

                if (fieldType.startsWithVarIntEncodedLength()) {
                    VarInt.writeVInt(os, length);
                }

                fieldData[i].getUnderlyingArray().writeTo(os, 0, length);
            } else {
                if(fieldType == FieldType.FLOAT) {
                    writeNullFloat(os);
                } else if(fieldType == FieldType.DOUBLE) {
                    writeNullDouble(os);
                } else {
                    os.write(0x80);
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
            FieldType fieldType = schema.getFieldType(i);
            if (isNonNull[i]) {
                if (fieldType.startsWithVarIntEncodedLength()) {
                    dataSize += VarInt.sizeOfVInt(fieldData[i].length());
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


    /**
     * Serialize a special 4-byte long sequence indicating a null Float value.
     * @throws IOException
     */
    private static void writeNullFloat(OutputStream os) throws IOException {
        os.write(NULL_FLOAT_BITS >>> 24);
        os.write(NULL_FLOAT_BITS >>> 16);
        os.write(NULL_FLOAT_BITS >>> 8);
        os.write(NULL_FLOAT_BITS);
    }

    /**
     * Serialize a special 4-byte long sequence indicating a null Float value.
     * @throws IOException
     */
    private static void writeNullDouble(OutputStream os) throws IOException {
        os.write((int) (NULL_DOUBLE_BITS >>> 56));
        os.write((int) (NULL_DOUBLE_BITS >>> 48));
        os.write((int) (NULL_DOUBLE_BITS >>> 40));
        os.write((int) (NULL_DOUBLE_BITS >>> 32));
        os.write((int) (NULL_DOUBLE_BITS >>> 24));
        os.write((int) (NULL_DOUBLE_BITS >>> 16));
        os.write((int) (NULL_DOUBLE_BITS >>> 8));
        os.write((int) NULL_DOUBLE_BITS);
    }
}
