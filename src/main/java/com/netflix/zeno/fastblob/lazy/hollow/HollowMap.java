package com.netflix.zeno.fastblob.lazy.hollow;

import com.netflix.zeno.fastblob.lazy.LazyStateEngine;
import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;

public class HollowMap extends HollowContainer {

    protected String keyType;
    protected String valueType;

    protected void position(LazyStateEngine stateEngine, String keyType, String valueType, ByteData data, long position) {
        this.stateEngine = stateEngine;
        this.keyType = keyType;
        this.valueType = valueType;
        this.data = data;
        this.position = position;
    }

    public int getOrdinal(int keyOrdinal) {
        int numBytes = VarInt.readVInt(data, position);
        long position = this.position + VarInt.sizeOfVInt(numBytes);
        long endPosition = position + numBytes;

        int valueOrdinal = 0;
        while(position < endPosition) {
            int readKeyOrdinal = -1;
            if(VarInt.readVNull(data, position)) {
                position++;
            } else {
                readKeyOrdinal = VarInt.readVInt(data, position);
                position += VarInt.sizeOfVInt(readKeyOrdinal);
            }

            int readValueOrdinalDelta = -1;
            if(VarInt.readVNull(data, position)) {
                position++;
            } else {
                readValueOrdinalDelta = VarInt.readVInt(data, position);
                position += VarInt.sizeOfVInt(readValueOrdinalDelta);
                valueOrdinal += readValueOrdinalDelta;
            }

            if(readKeyOrdinal == keyOrdinal)
                return valueOrdinal;
        }

        return -1;
    }

    public int size() {
        int numBytes = VarInt.readVInt(data, position);
        long position = this.position + VarInt.sizeOfVInt(numBytes);

        return VarInt.countVarIntsInRange(data, position, numBytes) / 2;
    }

    public HollowMapEntryOrdinalIterator hollowEntryOrdinalIterator() {
        return new HollowMapEntryOrdinalIterator(data, position);
    }

}
