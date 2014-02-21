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

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.Collection;

public class AState1Serializer extends NFTypeSerializer<TypeAState1>{

    public AState1Serializer() {
        super("TypeA");
    }

    @Override
    public void doSerialize(TypeAState1 value, NFSerializationRecord rec) {
        serializePrimitive(rec, "a1", value.getA1());
    }

    @Override
    protected TypeAState1 doDeserialize(NFDeserializationRecord rec) {
        int a1 = deserializeInteger(rec, "a1");
        return new TypeAState1(a1);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("a1", FieldType.INT)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers();
    }

}
