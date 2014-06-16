package com.netflix.zeno.fastblob.lazy.hollow;

import com.netflix.zeno.fastblob.record.VarInt;

public class HollowMap extends HollowContainer {

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

}
