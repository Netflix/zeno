/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.zeno.testpojos;

import java.util.Collection;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

public class TypeDSerializer extends NFTypeSerializer<TypeD> {

    private final FastBlobSchemaField[] fields = new FastBlobSchemaField[] {
            field("val", FieldType.INT),
            field("a", new TypeASerializer())
    };

    public TypeDSerializer() {
        super("TypeD");
    }

    @Override
    public void doSerialize(TypeD value, NFSerializationRecord rec) {
        serializePrimitive(rec, "val", value.getVal());
        serializeObject(rec, "a", value.getTypeA());
    }

    @Override
    protected TypeD doDeserialize(NFDeserializationRecord rec) {
        return new TypeD(
                deserializeInteger(rec, "val"),
                (TypeA) deserializeObject( rec, "a")
        );
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
