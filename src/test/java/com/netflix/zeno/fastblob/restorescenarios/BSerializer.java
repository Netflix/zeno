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

public class BSerializer extends NFTypeSerializer<TypeB> {

    public BSerializer() {
        super("TypeB");
    }

    @Override
    public void doSerialize(TypeB value, NFSerializationRecord rec) {
        serializePrimitive(rec, "b1", value.getB1());
    }

    @Override
    protected TypeB doDeserialize(NFDeserializationRecord rec) {
        int b1 = deserializeInteger(rec, "b1");

        return new TypeB(b1);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("b1", FieldType.INT)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers();
    }





}
