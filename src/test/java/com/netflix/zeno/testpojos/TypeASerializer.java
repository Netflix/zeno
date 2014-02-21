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

import java.util.Collection;
import java.util.Collections;

public class TypeASerializer extends NFTypeSerializer<TypeA>{

    public TypeASerializer() {
        super("TypeA");
    }

    @Override
    public void doSerialize(TypeA obj, NFSerializationRecord rec) {
        serializePrimitive(rec, "val1", obj.getVal1());
        serializePrimitive(rec, "val2", obj.getVal2());
    }

    @Override
    protected TypeA doDeserialize(NFDeserializationRecord rec) {
        int val1 = deserializeInteger(rec, "val1");
        int val2 = deserializeInteger(rec, "val2");
        return new TypeA(val1, val2);
    }

    @Override
    public FastBlobSchema createSchema() {
        return schema(
                field("val1", FieldType.INT),
                field("val2", FieldType.INT)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return Collections.emptyList();
    }

}
