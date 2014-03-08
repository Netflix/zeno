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
package com.netflix.zeno.diff;

import com.netflix.zeno.serializer.FrameworkSerializer;
import com.netflix.zeno.serializer.SerializationFramework;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *
 * Defines operations required to populate individual POJO primitive elements into DiffRecords
 *
 * @author dkoszewnik
 *
 */
public class DiffFrameworkSerializer extends FrameworkSerializer<DiffRecord> {

    private static final Object NULL_OBJECT = new Integer(Integer.MIN_VALUE + 100);

    public DiffFrameworkSerializer(SerializationFramework framework) {
        super(framework);
    }

    @Override
    public void serializePrimitive(DiffRecord rec, String fieldName, Object value) {
        rec.serializePrimitive(fieldName, value);
    }

    @Override
    public void serializeBytes(DiffRecord rec, String fieldName, byte[] value) {
        rec.serializePrimitive(fieldName, new DiffByteArray(value));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void serializeObject(DiffRecord rec, String fieldName, String typeName, Object obj) {
        if(obj == null) {
            serializePrimitive(rec, fieldName, NULL_OBJECT);
            return;
        }

        rec.serializeObject(fieldName);

        getSerializer(typeName).serialize(obj, rec);

        rec.finishedObject();
    }

    @Override
    public void serializeObject(DiffRecord rec, String fieldName, Object obj) {
        serializeObject(rec, fieldName, rec.getSchema().getObjectType(fieldName), obj);
    }

    @Override
    public <T> void serializeList(DiffRecord rec, String fieldName, String typeName, Collection<T> obj) {
        serializeCollection(rec, fieldName, typeName, obj);
    }

    @Override
    public <T> void serializeSet(DiffRecord rec, String fieldName, String typeName, Set<T> obj) {
        serializeCollection(rec, fieldName, typeName, obj);
    }

    private <T> void serializeCollection(DiffRecord rec, String fieldName, String typeName, Collection<T> obj) {
        if(obj == null) {
            serializePrimitive(rec, fieldName, NULL_OBJECT);
            return;
        }

        rec.serializeObject(fieldName);

        for(T t : obj) {
            serializeObject(rec, "element", typeName, t);
        }

        rec.finishedObject();
    }

    @Override
    public <K, V> void serializeMap(DiffRecord rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> obj) {
        if(obj == null) {
            serializePrimitive(rec, fieldName, NULL_OBJECT);
            return;
        }

        rec.serializeObject(fieldName);

        for(Map.Entry<K, V> entry : obj.entrySet()) {
            serializeObject(rec, "key", keyTypeName, entry.getKey());
            serializeObject(rec, "value", valueTypeName, entry.getValue());
        }

        rec.finishedObject();
    }

}
