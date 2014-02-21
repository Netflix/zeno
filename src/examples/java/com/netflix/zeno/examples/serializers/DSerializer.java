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
import com.netflix.zeno.examples.pojos.D;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.common.IntegerSerializer;
import com.netflix.zeno.serializer.common.ListSerializer;
import com.netflix.zeno.serializer.common.MapSerializer;
import com.netflix.zeno.serializer.common.SetSerializer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DSerializer extends NFTypeSerializer<D> {

    public DSerializer() {
        super("D");
    }

    @Override
    public void doSerialize(D value, NFSerializationRecord rec) {
        serializeObject(rec, "aList", value.getList());
        serializeObject(rec, "bSet", value.getSet());
        serializeObject(rec, "cMap", value.getMap());
    }

    @Override
    protected D doDeserialize(NFDeserializationRecord rec) {
        List<A> aList = deserializeObject(rec, "aList");
        Set<B> bSet = deserializeObject(rec, "bSet");
        Map<Integer, C> cMap = deserializeObject(rec, "cMap");

        return new D(aList, bSet, cMap);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("aList", "ListOfAs"),
                field("bSet", "SetOfBs"),
                field("cMap", "MapOfCs")
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers(
                new ListSerializer<A>("ListOfAs", new ASerializer()),
                new SetSerializer<B>("SetOfBs", new BSerializer()),
                new MapSerializer<Integer, C>("MapOfCs", new IntegerSerializer(), new CSerializer())
        );
    }

}
