package com.netflix.zeno.fastblob.lazy.hollow;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;

public class HollowListOrdinalIterator implements HollowOrdinalIterator {

    private final ByteData data;
    private long position;
    private final long endPosition;

    public HollowListOrdinalIterator(ByteData data, long position) {
        this.data = data;
        int totalBytes = VarInt.readVInt(data, position);
        this.position = position + VarInt.sizeOfVInt(totalBytes);
        this.endPosition = this.position + totalBytes;
    }

    public int next() {
        if(position >= endPosition)
            return NO_MORE_ORDINALS;

        if(VarInt.readVNull(data, position)) {
            position++;
            return -1;
        }

        int nextOrdinal = VarInt.readVInt(data, position);
        position += VarInt.sizeOfVInt(nextOrdinal);
        return nextOrdinal;
    }

}
