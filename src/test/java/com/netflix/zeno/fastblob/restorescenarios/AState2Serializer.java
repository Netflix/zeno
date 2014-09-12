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
package com.netflix.zeno.fastblob.restorescenarios;

import java.util.Collection;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

public class AState2Serializer extends NFTypeSerializer<TypeAState2>{

    private final FastBlobSchemaField[] fields = new FastBlobSchemaField[] {
            field("a1", FieldType.INT),
            field("c", new TypeCSerializer())
    };

    public AState2Serializer() {
        super("TypeA");
    }

    @Override
    public void doSerialize(TypeAState2 value, NFSerializationRecord rec) {
        serializePrimitive(rec, "a1", value.getA1());
        serializeObject(rec, "c", value.getC());
    }

    @Override
    protected TypeAState2 doDeserialize(NFDeserializationRecord rec) {
        int a1 = deserializeInteger(rec, "a1");
        TypeC c = deserializeObject(rec, "c");

        return new TypeAState2(a1, c);
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
