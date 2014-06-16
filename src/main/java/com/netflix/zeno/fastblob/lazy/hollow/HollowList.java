package com.netflix.zeno.fastblob.lazy.hollow;

import com.netflix.zeno.fastblob.record.VarInt;

public class HollowList extends HollowCollection {

    public HollowObject getObject(int index) {
        int ordinal = getOrdinal(index);
        if(ordinal == -1)
            return null;
        return stateEngine.getHollowObject(elementType, ordinal);
    }

    public int getOrdinal(int index) {
        int numBytes = VarInt.readVInt(data, position);
        long position = this.position + VarInt.sizeOfVInt(numBytes);

        int readBytes = 0;
        int ordinal = -1;
        for(int i=0;i<index+1;i++) {
            if(readBytes >= numBytes)
                throw new ArrayIndexOutOfBoundsException(index);

            if(VarInt.readVNull(data, position)) {
                ordinal = -1;
                position++;
                readBytes++;
            } else {
                ordinal = VarInt.readVInt(data, position);
                int sizeOfVInt = VarInt.sizeOfVInt(ordinal);
                position += sizeOfVInt;
                readBytes += sizeOfVInt;
            }
        }

        return ordinal;
    }

    public HollowOrdinalIterator ordinalIterator() {
        return new HollowListOrdinalIterator(data, position);
    }
}
