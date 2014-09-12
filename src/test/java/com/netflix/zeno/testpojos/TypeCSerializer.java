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
import java.util.List;
import java.util.Map;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.ListSerializer;
import com.netflix.zeno.serializer.common.MapSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

public class TypeCSerializer extends NFTypeSerializer<TypeC> {

    public static final ListSerializer<TypeB> LIST_SERIALIZER = new ListSerializer<TypeB>(new TypeBSerializer());
    public static final MapSerializer<String, TypeA> MAP_SERIALIZER = new MapSerializer<String, TypeA>(new StringSerializer(), new TypeASerializer());

    private final FastBlobSchemaField[] fields = new FastBlobSchemaField[] {
            field("typeA", new MapSerializer<String, TypeA>(new StringSerializer(), new TypeASerializer())),
            field("typeB", new ListSerializer<TypeB>(new TypeBSerializer()))
    };

    public TypeCSerializer() {
        super("TypeC");
    }

    @Override
    public void doSerialize(TypeC value, NFSerializationRecord rec) {
        serializeObject(rec, "typeA", value.getTypeAMap());
        serializeObject(rec, "typeB", value.getTypeBs());

    }

    @SuppressWarnings("unchecked")
    @Override
    protected TypeC doDeserialize(NFDeserializationRecord rec) {
        return new TypeC(
                (Map<String, TypeA>) deserializeObject(rec, "typeA"),
                (List<TypeB>) deserializeObject(rec, "typeB")
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
