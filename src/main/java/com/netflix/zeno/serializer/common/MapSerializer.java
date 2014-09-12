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
package com.netflix.zeno.serializer.common;

import java.util.Collection;
import java.util.Map;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;

/**
*
* A default NFTypeSerializer implementation for Map objects.
*
*/
public class MapSerializer<K, V> extends NFTypeSerializer<Map<K, V>> {

    private final NFTypeSerializer<K> keySerializer;
    private final NFTypeSerializer<V> valueSerializer;

    public MapSerializer(String schemaName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        super(schemaName);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public MapSerializer(NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        this("MapOf" + keySerializer.getName() + "To" + valueSerializer.getName(), keySerializer, valueSerializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void doSerialize(Map<K, V> map, NFSerializationRecord rec) {
        serializationFramework.getFrameworkSerializer().serializeMap(rec, "map", keySerializer.getName(), valueSerializer.getName(), map);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Map<K, V> doDeserialize(NFDeserializationRecord rec) {
        return serializationFramework.getFrameworkDeserializer().deserializeMap(rec, "map", keySerializer, valueSerializer);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
            mapField("map", keySerializer.getName(), valueSerializer.getName())
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers(
                keySerializer,
                valueSerializer
        );
    }

    @Override
    public void setSerializationFramework(SerializationFramework framework) {
        this.serializationFramework = framework;
        keySerializer.setSerializationFramework(framework);
        valueSerializer.setSerializationFramework(framework);
    }
}
