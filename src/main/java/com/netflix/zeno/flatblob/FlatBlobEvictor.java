package com.netflix.zeno.flatblob;

import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

public class FlatBlobEvictor extends SerializationFramework {

    private final NFSerializationRecord THE_RECORD = new NFSerializationRecord() { };

    public FlatBlobEvictor(SerializerFactory serializerFactory, FlatBlobSerializationFramework flatBlobFramework) {
        super(serializerFactory);
        this.frameworkSerializer = new FlatBlobEvictionFrameworkSerializer(this, flatBlobFramework);
    }

    public void evict(String type, Object obj) {
        getSerializer(type).serialize(obj, THE_RECORD);
    }



}
