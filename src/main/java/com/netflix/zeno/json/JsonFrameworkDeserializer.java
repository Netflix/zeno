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

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.zeno.serializer.FrameworkDeserializer;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Base64;

public class JsonFrameworkDeserializer extends FrameworkDeserializer<JsonReadGenericRecord> {

    JsonFrameworkDeserializer(JsonSerializationFramework framework) {
        super(framework);
    }

    @Override
    public Boolean deserializeBoolean(JsonReadGenericRecord rec, String fieldName) {
        JsonReadGenericRecord record = (JsonReadGenericRecord) rec;
        JsonNode node = record.getNode().isBoolean() ? record.getNode() : getJsonNode(rec, fieldName);
        if (node == null)
            return null;
        return node.booleanValue();
    }

    @Override
    public Integer deserializeInteger(JsonReadGenericRecord record, String fieldName) {
        JsonNode node = record.getNode().isNumber() ? record.getNode() : getJsonNode(record, fieldName);
        if (node == null)
            return null;
        return node.intValue();
    }

    @Override
    public Long deserializeLong(JsonReadGenericRecord record, String fieldName) {
        JsonNode node = record.getNode().isNumber() ? record.getNode() : getJsonNode(record, fieldName);
        if (node == null)
            return null;
        return node.longValue();
    }

    @Override
    public Float deserializeFloat(JsonReadGenericRecord record, String fieldName) {
        JsonNode node = record.getNode().isNumber() ? record.getNode() : getJsonNode(record, fieldName);
        if (node == null)
            return null;
        return node.numberValue().floatValue();
    }

    @Override
    public Double deserializeDouble(JsonReadGenericRecord record, String fieldName) {
        JsonNode node = record.getNode().isNumber() ? record.getNode() : getJsonNode(record, fieldName);
        if (node == null)
            return null;
        return node.numberValue().doubleValue();
    }

    @Override
    public String deserializeString(JsonReadGenericRecord record, String fieldName) {
        JsonNode node = record.getNode().isTextual() ? record.getNode() : getJsonNode(record, fieldName);
        if (node == null)
            return null;
        return node.textValue();
    }

    /**
     * @deprecated use instead deserializeObject(JsonReadGenericRecord rec, String fieldName, Class<T> clazz);
     */
    @Deprecated
    @Override
    public <T> T deserializeObject(JsonReadGenericRecord rec, String fieldName, String typeName, Class<T> clazz) {
        JsonNode node = getJsonNode(rec, fieldName);
        if (node == null)
            return null;
        return deserializeObject(rec, typeName, node);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> T deserializeObject(JsonReadGenericRecord rec, String typeName, JsonNode node) {
        NFTypeSerializer serializer = ((NFTypeSerializer) (framework.getSerializer(typeName)));
        return (T) serializer.deserialize(new JsonReadGenericRecord(serializer.getFastBlobSchema(), node));
    }

    @Override
    public <T> T deserializeObject(JsonReadGenericRecord rec, String fieldName, Class<T> clazz) {
        JsonNode node = getJsonNode(rec, fieldName);
        if (node == null)
            return null;
        return deserializeObject(rec, rec.getObjectType(fieldName), node);
    }

    @Override
    public <T> List<T> deserializeList(JsonReadGenericRecord record, String fieldName, NFTypeSerializer<T> itemSerializer) {
        JsonNode node = getJsonNode(record, "list");
        if (node == null)
            return null;
        List<T> list = new ArrayList<T>();
        deserializeCollection(node, itemSerializer, list);
        return list;
    }

    @Override
    public <T> Set<T> deserializeSet(JsonReadGenericRecord record, String fieldName, NFTypeSerializer<T> itemSerializer) {
        JsonNode node = getJsonNode(record, "set");
        if (node == null)
            return null;
        Set<T> set = new HashSet<T>();
        deserializeCollection(node, itemSerializer, set);
        return set;
    }

    private JsonNode getJsonNode(Object rec, String fieldName) {
        JsonReadGenericRecord record = (JsonReadGenericRecord) rec;
        JsonNode node = record.getNode().get(fieldName);
        if (node == null || node.isNull()) {
            return null;
        }
        return node;
    }

    private <T> void deserializeCollection(JsonNode nodes, NFTypeSerializer<T> itemSerializer, Collection<T> elements) {
        try {
            for (Iterator<JsonNode> it = nodes.elements(); it.hasNext();) {
                JsonNode node = it.next();
                T element = itemSerializer.deserialize(new JsonReadGenericRecord(itemSerializer.getFastBlobSchema(), node));
                elements.add(element);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public <K, V> Map<K, V> deserializeMap(JsonReadGenericRecord record, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        return deserializeIntoMap(record, fieldName, keySerializer, valueSerializer, new HashMap<K, V>());

    }

    @Override
    public byte[] deserializeBytes(JsonReadGenericRecord record, String fieldName) {
        String str = deserializeString(record, fieldName);
        if (str == null) {
            return null;
        }
        return Base64.decodeBase64(str);
    }

    @Override
    public <K, V> SortedMap<K, V> deserializeSortedMap(JsonReadGenericRecord record, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        return deserializeIntoMap(record, fieldName, keySerializer, valueSerializer, new TreeMap<K, V>());
    }

    private <K, V, M extends Map<K, V>> M deserializeIntoMap(JsonReadGenericRecord rec, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer, M map) {
        JsonNode node = getJsonNode(rec, fieldName);
        if (node == null) {
            return null;
        }
        for (Iterator<JsonNode> it = node.elements(); it.hasNext();) {
            JsonNode element = it.next();
            K key = keySerializer.deserialize(new JsonReadGenericRecord(keySerializer.getFastBlobSchema(), element.get("key")));
            V value = valueSerializer.deserialize(new JsonReadGenericRecord(valueSerializer.getFastBlobSchema(), element.get("value")));
            map.put(key, value);
        }
        return map;
    }


}
