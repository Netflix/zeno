package com.netflix.zeno.serializer;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;

public class AbstractNFDeserializationRecord implements NFDeserializationRecord{
    private final FastBlobSchema schema;
    
    public AbstractNFDeserializationRecord(FastBlobSchema schema) {
        this.schema = schema;
    }

    public String getObjectType(String fieldName) {
        return schema.getObjectType(fieldName);
    }
    
    public FastBlobSchema getSchema() {
        return schema;
    }
}
