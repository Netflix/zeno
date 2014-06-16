package com.netflix.zeno.fastblob.lazy.hollow;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;

public class HollowMapEntryIterator {

    private final ByteData data;
    private long position;
    private final long endPosition;

    private int keyOrdinal = -1;
    private int valueOrdinal = -1;

    private int previousValueOrdinal = 0;

    public HollowMapEntryIterator(ByteData data, long position) {
        this.data = data;
        int totalBytes = VarInt.readVInt(data, position);
        this.position = position + VarInt.sizeOfVInt(totalBytes);
        this.endPosition = this.position + totalBytes;
    }

    public boolean next() {
        if(position >= endPosition)
            return false;

        if(VarInt.readVNull(data, position)) {
            keyOrdinal = -1;
            position++;
        } else {
            keyOrdinal = VarInt.readVInt(data, position);
            position += VarInt.sizeOfVInt(keyOrdinal);
        }

        if(VarInt.readVNull(data, position)) {
            valueOrdinal = -1;
            position++;
        } else {
            int valueOrdinalDelta = VarInt.readVInt(data, position);
            position += VarInt.sizeOfVInt(valueOrdinalDelta);
            valueOrdinal = previousValueOrdinal + valueOrdinalDelta;
            previousValueOrdinal = valueOrdinal;
        }

        return true;
    }

    public int getKeyOrdinal() {
        return keyOrdinal;
    }

    public int getValueOrdinal() {
        return valueOrdinal;
    }


}
