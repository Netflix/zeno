package com.netflix.zeno.fastblob.lazy.schema;

import com.netflix.zeno.fastblob.lazy.serialize.LazyBlobSerializer;
import com.netflix.zeno.fastblob.lazy.serialize.LazyTypeSerializationState;
import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FieldDefinition;
import com.netflix.zeno.fastblob.record.schema.MapFieldDefinition;
import com.netflix.zeno.fastblob.record.schema.TypedFieldDefinition;

import java.util.Arrays;

public class OrdinalRemapper {

    private final LazyBlobSerializer lazySerializer;
    private final ByteDataBuffer scratch;

    public OrdinalRemapper(LazyBlobSerializer lazySerializer) {
        this.lazySerializer = lazySerializer;
        this.scratch = new ByteDataBuffer();
    }

    ///TODO: TEST!  With null elements!
    public void remapOrdinals(FastBlobDeserializationRecord rec, ByteDataBuffer toSpace) {
        FastBlobSchema schema = rec.getSchema();
        ByteData fromSpace = rec.getByteData();

        long currentPointerPosition = rec.getPosition(0);

        for(int i=0;i<schema.numFields();i++) {
            FieldDefinition fieldDef = schema.getFieldDefinition(i);
            int length = rec.getFieldLength(i);

            TypedFieldDefinition typedFieldDef;
            int ordinal;
            int mappedOrdinal;

            switch(fieldDef.getFieldType()) {
            case OBJECT:
                typedFieldDef = (TypedFieldDefinition)fieldDef;

                if(VarInt.readVNull(fromSpace, currentPointerPosition)) {
                   VarInt.writeVNull(toSpace);
                   currentPointerPosition++;
                } else {
                    ordinal = VarInt.readVInt(fromSpace, currentPointerPosition);
                    currentPointerPosition += VarInt.sizeOfVInt(ordinal);

                    mappedOrdinal = getMappedOrdinal(typedFieldDef.getSubType(), ordinal);

                    VarInt.writeVInt(toSpace, mappedOrdinal);
                }
                break;
            case SET:
                typedFieldDef = (TypedFieldDefinition)fieldDef;
                currentPointerPosition = copySetWithRemappedOrdinals(fromSpace, currentPointerPosition, toSpace, typedFieldDef.getSubType());
                break;
            case LIST:
                typedFieldDef = (TypedFieldDefinition)fieldDef;

                currentPointerPosition = copyListWithRemappedOrdinals(toSpace, fromSpace, currentPointerPosition, typedFieldDef.getSubType());

                break;
            case MAP:
                MapFieldDefinition mapFieldDef = (MapFieldDefinition)fieldDef;
                currentPointerPosition = copyMapWithRemappedOrdinals(toSpace, fromSpace, currentPointerPosition, mapFieldDef);

                break;
            default:
                toSpace.copyFrom(fromSpace, currentPointerPosition, length);
                currentPointerPosition += length;
            }
        }
    }

    private long copyListWithRemappedOrdinals(ByteDataBuffer toSpace, ByteData fromSpace, long pointer, String elementType) {
        int sizeOfData = VarInt.readVInt(fromSpace, pointer);
        pointer += VarInt.sizeOfVInt(sizeOfData);
        int readBytesCounter = 0;

        while(readBytesCounter < sizeOfData) {
            if(VarInt.readVNull(fromSpace, pointer)) {
                VarInt.writeVNull(scratch);
                pointer++;
                readBytesCounter++;
            } else {
                int ordinal = VarInt.readVInt(fromSpace, pointer);
                int sizeOfOrdinal = VarInt.sizeOfVInt(ordinal);
                pointer += sizeOfOrdinal;
                readBytesCounter += sizeOfOrdinal;

                int mappedOrdinal = getMappedOrdinal(elementType, ordinal);
                VarInt.writeVInt(scratch, mappedOrdinal);
            }
        }

        VarInt.writeVInt(toSpace, (int)scratch.length());
        toSpace.copyFrom(scratch.getUnderlyingArray(), 0L, (int)scratch.length());
        scratch.reset();
        return pointer;
    }

    private long copySetWithRemappedOrdinals(ByteData fromSpace, long pointer, ByteDataBuffer toSpace, String elementType) {
        int sizeOfData = VarInt.readVInt(fromSpace, pointer);
        pointer += VarInt.sizeOfVInt(sizeOfData);
        int readBytesCounter = 0;
        int readOrdinalsCounter = 0;
        int currentOrdinal = 0;

        int mappedOrdinals[] = new int[sizeOfData];

        while(readBytesCounter < sizeOfData) {
            if(VarInt.readVNull(fromSpace, pointer)) {
                mappedOrdinals[readOrdinalsCounter++] = -1;
                pointer++;
                readBytesCounter++;
            } else {
                int ordinalDelta = VarInt.readVInt(fromSpace, pointer);
                int sizeOfOrdinalDelta = VarInt.sizeOfVInt(ordinalDelta);
                pointer += sizeOfOrdinalDelta;
                readBytesCounter += sizeOfOrdinalDelta;
                currentOrdinal += ordinalDelta;
                int mappedOrdinal = getMappedOrdinal(elementType, currentOrdinal);
                mappedOrdinals[readOrdinalsCounter++] = mappedOrdinal;
            }
        }

        Arrays.sort(mappedOrdinals, 0, readOrdinalsCounter);
        currentOrdinal = 0;

        for(int j=0;j<readOrdinalsCounter;j++) {
            if(mappedOrdinals[j] == -1) {
                VarInt.writeVNull(scratch);
            } else {
                VarInt.writeVInt(scratch, mappedOrdinals[j] - currentOrdinal);
                currentOrdinal = mappedOrdinals[j];
            }
        }

        VarInt.writeVInt(toSpace, (int)scratch.length());
        toSpace.copyFrom(scratch.getUnderlyingArray(), 0L, (int)scratch.length());
        scratch.reset();

        return pointer;
    }

    private long copyMapWithRemappedOrdinals(ByteDataBuffer toSpace, ByteData fromSpace, long pointer, MapFieldDefinition mapFieldDef) {
        int sizeOfData = VarInt.readVInt(fromSpace, pointer);
        long mapEntries[] = new long[sizeOfData / 2];
        pointer += VarInt.sizeOfVInt(sizeOfData);

        int readBytesCounter = 0;
        int currentValueOrdinal = 0;
        int readMapEntries = 0;


        while(readBytesCounter < sizeOfData) {
            int keyOrdinal = -1;
            int sizeOfKeyOrdinal = 1;
            if(VarInt.readVNull(fromSpace, pointer)) {
               pointer++;
            } else {
                keyOrdinal = VarInt.readVInt(fromSpace, pointer);
                sizeOfKeyOrdinal = VarInt.sizeOfVInt(keyOrdinal);
                pointer += sizeOfKeyOrdinal;
            }

            int valueOrdinalDelta = -1;
            int sizeOfValueOrdinalDelta = 1;
            if(VarInt.readVNull(fromSpace, pointer)) {
                pointer++;
            } else {
                valueOrdinalDelta = VarInt.readVInt(fromSpace, pointer);
                sizeOfValueOrdinalDelta = VarInt.sizeOfVInt(valueOrdinalDelta);
                pointer += sizeOfValueOrdinalDelta;
                currentValueOrdinal += valueOrdinalDelta;
            }


            int mappedKeyOrdinal = keyOrdinal == -1 ? -1 : getMappedOrdinal(mapFieldDef.getKeyType(), keyOrdinal);
            int mappedValueOrdinal = valueOrdinalDelta == -1 ? -1 : getMappedOrdinal(mapFieldDef.getValueType(), currentValueOrdinal);

            mapEntries[readMapEntries++] = mappedValueOrdinal == -1 ? 0xFFFFFFFF00000000L | mappedKeyOrdinal : ((long)mappedValueOrdinal << 32) | (mappedKeyOrdinal & 0xFFFFFFFFL);

            readBytesCounter += sizeOfKeyOrdinal + sizeOfValueOrdinalDelta;
        }

        Arrays.sort(mapEntries);

        currentValueOrdinal = 0;

        for(int j=0;j<readMapEntries;j++) {
            int valueOrdinal = (int)(mapEntries[j] >> 32);
            int keyOrdinal = (int)(mapEntries[j] & 0xFFFFFFFFL);

            if(keyOrdinal == -1)
                VarInt.writeVNull(scratch);
            else
                VarInt.writeVInt(scratch, keyOrdinal);

            if(valueOrdinal == -1) {
                VarInt.writeVNull(scratch);
            } else {
                VarInt.writeVInt(scratch, valueOrdinal - currentValueOrdinal);
                currentValueOrdinal = valueOrdinal;
            }
        }

        VarInt.writeVInt(toSpace, (int)scratch.length());
        toSpace.copyFrom(scratch.getUnderlyingArray(), 0L, (int)scratch.length());
        scratch.reset();
        return pointer;
    }

    private int getMappedOrdinal(String type, int fromOrdinal) {
        LazyTypeSerializationState typeState = lazySerializer.getTypeState(type);
        return typeState.getMappedOrdinal(fromOrdinal);
    }

}
