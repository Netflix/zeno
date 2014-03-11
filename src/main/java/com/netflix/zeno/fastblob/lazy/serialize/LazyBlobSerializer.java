package com.netflix.zeno.fastblob.lazy.serialize;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;

import java.util.HashMap;
import java.util.Map;

public class LazyBlobSerializer {

    private final Map<String, LazyTypeSerializationState> lazySerializationStates;

    public LazyBlobSerializer() {
        lazySerializationStates = new HashMap<String, LazyTypeSerializationState>();
    }

    public void addInstance(String type, int ordinal, ByteDataBuffer data) {
        LazyTypeSerializationState state = lazySerializationStates.get(type);
        state.addRecord(ordinal, data);
    }

    public void removeInstance(String type, int ordinal) {
        LazyTypeSerializationState state = lazySerializationStates.get(type);
        state.removeRecord(ordinal);
    }

    public LazyTypeSerializationState getTypeState(String typeName) {
        LazyTypeSerializationState typeState = lazySerializationStates.get(typeName);
        if(typeState == null) {
            typeState = new LazyTypeSerializationState();
            lazySerializationStates.put(typeName, typeState);
        }
        return typeState;
    }
}
