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

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;

public class FlatBlobDeserializationRecord extends NFDeserializationRecord {

    private final long fieldPointers[];
    private ByteData byteData;
    private boolean cacheElements;

    public FlatBlobDeserializationRecord(FastBlobSchema schema) {
        super(schema);
        this.fieldPointers = new long[schema.numFields()];
    }

    public void setByteData(ByteData byteData) {
        this.byteData = byteData;
    }

    public void setCacheElements(boolean cacheElements) {
        this.cacheElements = cacheElements;
    }

    public boolean shouldCacheElements() {
        return cacheElements;
    }

    /**
     * Position this record to the byte at index <code>objectBeginOffset</code>.
     *
     * @param objectBeginOffset
     * @return The length of the object's data, in bytes.
     */
    public int position(long objectBeginOffset) {
        long currentPosition = objectBeginOffset;

        for(int i=0;i<fieldPointers.length;i++) {
            fieldPointers[i] = currentPosition;

            FieldType type = getSchema().getFieldType(i);

            currentPosition += fieldLength(currentPosition, type);
        }

        return (int)(currentPosition - objectBeginOffset);
    }

    /**
     * Get the underlying byte data where this record is contained.
     */
    public ByteData getByteData() {
        return byteData;
    }

    /**
     * get the offset into the byte data for the field represented by the String.
     */
    public long getPosition(String fieldName) {
        int fieldPosition = getSchema().getPosition(fieldName);

        if(fieldPosition == -1)
            return -1;

        return fieldPointers[fieldPosition];
    }


    /**
     * get the offset into the byte data for the field represented by the String.
     */
    @Override
    public String getObjectType(String fieldName) {
        return getSchema().getObjectType(fieldName);
    }

    private int fieldLength(long currentPosition, FieldType type) {
        if(type.startsWithVarIntEncodedLength()) {
            if(VarInt.readVNull(byteData, currentPosition)) {
                return 1;
            } else {
                int fieldLength = VarInt.readVInt(byteData, currentPosition);
                return VarInt.sizeOfVInt(fieldLength) + fieldLength;
            }
        } else if(type.equals(FieldType.OBJECT)) {
            if(VarInt.readVNull(byteData, currentPosition)) {
                return 1;
            } else {
                int ordinal = VarInt.readVInt(byteData, currentPosition);
                int sizeOfOrdinal = VarInt.sizeOfVInt(ordinal);

                if(VarInt.readVNull(byteData, currentPosition + sizeOfOrdinal)) {
                    System.out.println(getSchema().getName());
                }

                int flatDataSize = VarInt.readVInt(byteData, currentPosition + sizeOfOrdinal);
                return VarInt.sizeOfVInt(flatDataSize) + sizeOfOrdinal + flatDataSize;
            }
        } else if(type.getFixedLength() != -1) {
            return type.getFixedLength();
        } else {
            if(VarInt.readVNull(byteData, currentPosition)) {
                return 1;
            } else {
                long value = VarInt.readVLong(byteData, currentPosition);
                return VarInt.sizeOfVLong(value);
            }
        }
    }

}
