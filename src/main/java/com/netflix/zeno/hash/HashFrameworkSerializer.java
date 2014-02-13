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
package com.netflix.zeno.hash;

import com.netflix.zeno.serializer.FrameworkSerializer;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author tvaliulin
 *
 */
public class HashFrameworkSerializer extends FrameworkSerializer<HashGenericRecord> {

    HashFrameworkSerializer(SerializationFramework framework) {
        super(framework);
    }

    @Override
    public void serializePrimitive(HashGenericRecord rec, String fieldName, Object value) {
        if (value == null) {
            return;
        }
        HashGenericRecord record = rec;
        record.put(fieldName, value);
    }

    @Override
    public void serializeBytes(HashGenericRecord rec, String fieldName, byte[] value) {
        serializePrimitive(rec, fieldName, value);
    }

    /*
     * @Deprecated instead use serializeObject(HashGenericRecord rec, String fieldName, Object obj)
     * 
     */
    @Deprecated
    @SuppressWarnings({ "unchecked" })
    @Override
    public void serializeObject(HashGenericRecord rec, String fieldName, String typeName, Object obj) {
        if (obj == null) {
            return;
        }
        getSerializer(typeName).serialize(obj, rec);
    }
    
    @Override
    public void serializeObject(HashGenericRecord rec, String fieldName, Object obj) {
        serializeObject(rec, fieldName, rec.getSchema().getObjectType(fieldName), obj);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <T> void serializeList(HashGenericRecord rec, String fieldName, String typeName, Collection<T> list) {
        if (list == null) {
            return;
        }
        rec.put(null, "[");
        for (T t : list) {
            ((NFTypeSerializer) (framework.getSerializer(typeName))).serialize(t, rec);
        }
        rec.put(null, "]");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <T> void serializeSet(HashGenericRecord rec, String fieldName, String typeName, Set<T> set) {
        if (set == null) {
            return;
        }
        rec.put(null, "<");
        HashGenericRecord independent = new HashGenericRecord(rec.getSchema(), new HashOrderIndependent());
        for (T t : set) {
            HashGenericRecord dependent = new HashGenericRecord(rec.getSchema(), new HashOrderDependent());
            ((NFTypeSerializer) (framework.getSerializer(typeName))).serialize(t, dependent);
            independent.put(null, dependent.hash());
        }
        rec.put(null, independent.hash());
        rec.put(null, ">");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <K, V> void serializeMap(HashGenericRecord rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> map) {
        if (map == null) {
            return;
        }
        rec.put(null, "{");
        HashGenericRecord independent = new HashGenericRecord(rec.getSchema(), new HashOrderIndependent());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            HashGenericRecord dependent = new HashGenericRecord(rec.getSchema(), new HashOrderDependent());
            ((NFTypeSerializer) (framework.getSerializer(keyTypeName))).serialize(entry.getKey(), dependent);
            ((NFTypeSerializer) (framework.getSerializer(valueTypeName))).serialize(entry.getValue(), dependent);
            independent.put(null, dependent.hash());
        }
        rec.put(null, independent.hash());
        rec.put(null, "}");
    }

}
