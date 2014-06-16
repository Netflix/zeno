package com.netflix.zeno.fastblob.lazy.hollow;

import com.netflix.zeno.fastblob.lazy.LazyStateEngine;
import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;

public abstract class HollowContainer {

    protected LazyStateEngine stateEngine;
    protected String elementType;
    protected ByteData data;
    protected long position;

    public void position(LazyStateEngine stateEngine, String elementType, ByteData data, long position) {
        this.stateEngine = stateEngine;
        this.elementType = elementType;
        this.data = data;
        this.position = position;
    }

    public boolean isEmpty() {
        int numBytes = VarInt.readVInt(data, position);
        return numBytes == 0;
    }

}
