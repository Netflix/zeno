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
package com.netflix.zeno.examples.serializers;

import com.netflix.zeno.examples.pojos.B;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.Collection;
import java.util.Collections;


public class BSerializer extends NFTypeSerializer<B> {

    public BSerializer() {
        super("B");
    }

    @Override
    public void doSerialize(B value, NFSerializationRecord rec) {
        serializePrimitive(rec, "bInt", value.getBInt());
        serializePrimitive(rec, "bString", value.getBString());
    }

    @Override
    protected B doDeserialize(NFDeserializationRecord rec) {
        int bInt = deserializeInteger(rec, "bInt");
        String bString = deserializePrimitiveString(rec, "bString");

        return new B(bInt, bString);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("bInt", FieldType.INT),
                field("bString", FieldType.STRING)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return Collections.emptySet();
    }

}
