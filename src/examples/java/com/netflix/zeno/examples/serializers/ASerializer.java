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

import com.netflix.zeno.examples.pojos.A;
import com.netflix.zeno.examples.pojos.B;
import com.netflix.zeno.examples.pojos.C;
import com.netflix.zeno.fastblob.record.FastBlobSchema;
import com.netflix.zeno.fastblob.record.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.ListSerializer;

import java.util.Collection;
import java.util.List;


public class ASerializer extends NFTypeSerializer<A> {

    public ASerializer() {
        super("A");
    }

    @Override
    public void doSerialize(A value, NFSerializationRecord rec) {
        serializeObject(rec, "blist", "ListOfBs", value.getBList());
        serializeObject(rec, "c", "C", value.getCValue());
        serializePrimitive(rec, "intVal", value.getIntValue());
    }

    @Override
    protected A doDeserialize(NFDeserializationRecord rec) {
        List<B> bList = deserializeObject(rec, "ListOfBs", "blist");
        C c = deserializeObject(rec, "C", "c");
        int intValue = deserializeInteger(rec, "intVal");

        return new A(bList, c, intValue);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("blist"),
                field("c"),
                field("intVal", FieldType.INT)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers(
                new ListSerializer<B>("ListOfBs", new BSerializer()),
                new CSerializer()
        );
    }

}
