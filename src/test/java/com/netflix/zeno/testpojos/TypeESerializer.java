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

import com.netflix.zeno.fastblob.record.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.Collection;

public class TypeESerializer extends NFTypeSerializer<TypeE> {

    public TypeESerializer() {
        super("TypeE");
    }

    @Override
    public void doSerialize(TypeE value, NFSerializationRecord rec) {
        serializeObject(rec, "typeF", "TypeF", value.getTypeF());
    }

    @Override
    protected TypeE doDeserialize(NFDeserializationRecord rec) {
        return new TypeE((TypeF) deserializeObject(rec, "TypeF", ("typeF")));
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("typeF")
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers(
                new TypeFSerializer()
        );
    }

}
