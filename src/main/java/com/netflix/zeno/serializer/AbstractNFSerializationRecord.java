package com.netflix.zeno.serializer;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;

public class AbstractNFSerializationRecord implements NFSerializationRecord {
    private final FastBlobSchema schema;
    
    public AbstractNFSerializationRecord(FastBlobSchema schema) {
        this.schema = schema;
    }

    public String getObjectType(String fieldName) {
        return schema.getObjectType(fieldName);
    }
    
    public FastBlobSchema getSchema() {
        return schema;
    }    
}
