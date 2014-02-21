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

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

import java.util.Collection;

public class TypeBSerializer extends NFTypeSerializer<TypeB>{

    public TypeBSerializer() {
        super("TypeB");
    }

    @Override
    public void doSerialize(TypeB obj, NFSerializationRecord rec) {
        serializePrimitive(rec, "val1", obj.getVal1());
        serializePrimitive(rec, "val2", obj.getVal2());
    }

    @Override
    protected TypeB doDeserialize(NFDeserializationRecord rec) {
        final int val1 = deserializeInteger(rec, "val1");
        final String val2 = deserializePrimitiveString(rec, "val2");
        return new TypeB(val1, val2);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("val1", FieldType.INT),
                field("val2", FieldType.STRING)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers(new StringSerializer());
    }

}
