package com.netflix.zeno.fastblob.lazy.hollow;

import com.netflix.zeno.fastblob.lazy.LazyStateEngine;
import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;

public abstract class HollowContainer {

    protected LazyStateEngine stateEngine;
    protected ByteData data;
    protected long position;

    public boolean isEmpty() {
        int numBytes = VarInt.readVInt(data, position);
        return numBytes == 0;
    }

}
