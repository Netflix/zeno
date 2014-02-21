package com.netflix.zeno.fastblob;

import com.netflix.zeno.fastblob.state.TypeDeserializationState;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

public abstract class FastBlobSerializationFramework extends SerializationFramework {

    public FastBlobSerializationFramework(SerializerFactory serializerFactory) {
        super(serializerFactory);
    }

    public abstract <T> TypeDeserializationState<T> getTypeDeserializationState(String name);

}
