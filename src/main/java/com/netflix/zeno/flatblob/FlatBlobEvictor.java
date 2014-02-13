package com.netflix.zeno.flatblob;

import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

public class FlatBlobEvictor extends SerializationFramework {

    public FlatBlobEvictor(SerializerFactory serializerFactory, FlatBlobSerializationFramework flatBlobFramework) {
        super(serializerFactory);
        this.frameworkSerializer = new FlatBlobEvictionFrameworkSerializer(this, flatBlobFramework);
    }

    public void evict(String type, Object obj) {
        FlatBlobSerializationRecord record = new FlatBlobSerializationRecord(getSerializer(type).getFastBlobSchema());        
        getSerializer(type).serialize(obj, record);
    }
}
