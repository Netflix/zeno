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
package com.netflix.zeno.json;

import com.netflix.zeno.serializer.FrameworkSerializer;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.util.PrimitiveObjectIdentifier;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

/**
 *
 * @author tvaliulin
 *
 */
public class JsonFrameworkSerializer extends FrameworkSerializer<JsonWriteGenericRecord> {

    JsonFrameworkSerializer(SerializationFramework framework) {
        super(framework);
    }

    @Override
    public void serializePrimitive(JsonWriteGenericRecord rec, String fieldName, Object value) {
        JsonWriteGenericRecord record = (JsonWriteGenericRecord) rec;
        try {
            if (value != null) {
                if (value.getClass().isEnum()) {
                    value = ((Enum<?>) value).name();
                }
            }
            record.getGenerator().writeObjectField(fieldName, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void serializeBytes(JsonWriteGenericRecord rec, String fieldName, byte[] value) {
        String str = null;
        if (value != null) {
            byte encoded[] = Base64.encodeBase64(value, false);
            str = new String(encoded, Charset.forName("UTF-8"));
        }
        serializePrimitive(rec, fieldName, str);
    }

    private static boolean isPrimitive(Class<?> type) {
        return type.isEnum() || PrimitiveObjectIdentifier.isPrimitiveOrWrapper(type);
    }

    /*
     * @Deprecated instead use serializeObject(HashGenericRecord rec, String fieldName, Object obj)
     *
     */
    @Deprecated
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void serializeObject(JsonWriteGenericRecord record, String fieldName, String typeName, Object obj) {
        try {
            if (obj == null) {
                record.getGenerator().writeObjectField(fieldName, null);
                return;
            }
            if (isPrimitive(obj.getClass())) {
                serializePrimitive(record, fieldName, obj);
                return;
            }
            record.getGenerator().writeFieldName(fieldName);
            record.getGenerator().writeStartObject();

            NFTypeSerializer fieldSerializer = getSerializer(typeName);
            JsonWriteGenericRecord fieldRecord = new JsonWriteGenericRecord(record.getGenerator());
            fieldSerializer.serialize(obj, fieldRecord);
            record.getGenerator().writeEndObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void serializeObject(JsonWriteGenericRecord record, String fieldName, Object obj) {
        serializeObject(record, fieldName, record.getObjectType(fieldName), obj);
    }

    @Override
    public <T> void serializeList(JsonWriteGenericRecord rec, String fieldName, String typeName, Collection<T> obj) {
        serializeCollection(rec, "list", typeName, obj);
    }

    @Override
    public <T> void serializeSet(JsonWriteGenericRecord rec, String fieldName, String typeName, Set<T> obj) {
        serializeCollection(rec, "set", typeName, obj);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> void serializeCollection(JsonWriteGenericRecord record, String fieldName, String typeName, Collection<T> obj) {
        try {
            if (obj == null) {
                record.getGenerator().writeObjectField(fieldName, null);
                return;
            }
            record.getGenerator().writeArrayFieldStart(fieldName);
            NFTypeSerializer elemSerializer = getSerializer(typeName);
            JsonWriteGenericRecord elemRecord = new JsonWriteGenericRecord(record.getGenerator());

            for (T t : obj) {
                record.getGenerator().writeStartObject();
                elemSerializer.serialize(t, elemRecord);
                record.getGenerator().writeEndObject();
            }
            record.getGenerator().writeEndArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public <K, V> void serializeMap(JsonWriteGenericRecord record, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> obj) {
        try {
            if (obj == null) {
                record.getGenerator().writeObjectField(fieldName, null);
                return;
            }
            record.getGenerator().writeFieldName("map");
            record.getGenerator().writeStartArray();

            for (Map.Entry<K, V> entry : sortedEntryList(obj)) {
                record.getGenerator().writeStartObject();
                serializeObject(record, "key", keyTypeName, entry.getKey());
                serializeObject(record, "value", valueTypeName, entry.getValue());
                record.getGenerator().writeEndObject();
            }
            record.getGenerator().writeEndArray();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    // / Impose a consistent ordering over the map entries. This allows the
    // diffs to match up better.
    private <K, V> List<Map.Entry<K, V>> sortedEntryList(Map<K, V> obj) {
        List<Map.Entry<K, V>> entryList = new ArrayList<Map.Entry<K, V>>(obj.entrySet());

        Collections.sort(entryList, new Comparator<Map.Entry<K, V>>() {
            @Override
            @SuppressWarnings({ "unchecked", "rawtypes" })
            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
                K k1 = o1.getKey();
                K k2 = o2.getKey();

                if (k1 instanceof Comparable) {
                    return ((Comparable) k1).compareTo(k2);
                }

                return k1.hashCode() - k2.hashCode();
            }
        });

        return entryList;
    }

}
