package com.netflix.zeno.testpojos;

import java.util.Collection;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

public class TypeGSerializer extends NFTypeSerializer<TypeG>{

    private final FastBlobSchemaField[] fields = new FastBlobSchemaField[] {
            field("typeD", new TypeDSerializer())
    };

    public TypeGSerializer() {
        super("TypeG");
    }

    @Override
    protected void doSerialize(TypeG value, NFSerializationRecord rec) {
        serializeObject(rec, "typeD", value.getTypeD());
    }

    @Override
    protected TypeG doDeserialize(NFDeserializationRecord rec) {
        TypeD typeD = deserializeObject(rec, "typeD");
        return new TypeG(typeD);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(fields);
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return requiredSubSerializers(fields);
    }
}
