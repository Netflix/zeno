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
package com.netflix.zeno.examples.framework;

import com.netflix.zeno.serializer.FrameworkSerializer;
import com.netflix.zeno.serializer.SerializationFramework;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * After defining our "serialization record", we need to implement a "framework serializer".
 *
 * @author dkoszewnik
 *
 */
public class IntSumFrameworkSerializer extends FrameworkSerializer<IntSumRecord> {

    public IntSumFrameworkSerializer(SerializationFramework framework) {
        super(framework);
    }

    /**
     * We need to implement serializePrimitive to describe what happens when we encounter one of the following types:
     * Integer, Long, Float, Double, Boolean, String.
     */
    @Override
    public void serializePrimitive(IntSumRecord rec, String fieldName, Object value) {
        /// only interested in int values.
        if(value instanceof Integer) {
            rec.addValue(((Integer) value).intValue());
        }
    }

    @Override
    public void serializeObject(IntSumRecord rec, String fieldName, Object obj) {
        String typeName = rec.getSchema().getObjectType(fieldName);
        serializeObject(rec, fieldName, typeName, obj);
    }

    @Override
    public void serializeObject(IntSumRecord rec, String fieldName, String typeName, Object obj) {
        ((IntSumFramework)framework).getSum(typeName, obj, rec);
    }

    @Override
    public <T> void serializeList(IntSumRecord rec, String fieldName, String typeName, Collection<T> obj) {
        serializeCollection(rec, obj, typeName);
    }

    @Override
    public <T> void serializeSet(IntSumRecord rec, String fieldName, String typeName, Set<T> obj) {
        serializeCollection(rec, obj, typeName);
    }

    @Override
    public <K, V> void serializeMap(IntSumRecord rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> obj) {
        serializeCollection(rec, obj.keySet(), keyTypeName);
        serializeCollection(rec, obj.values(), valueTypeName);
    }

    private <T> void serializeCollection(IntSumRecord rec, Collection<T> coll, String elementTypeName) {
        for(T t : coll) {
            ((IntSumFramework)framework).getSum(elementTypeName, t, rec);
        }
    }

    @Override
    public void serializeBytes(IntSumRecord rec, String fieldName, byte[] value) {
        // do nothing, not interested in byte[]
    }

}
