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
package com.netflix.zeno.serializer;

import com.netflix.zeno.hash.HashFrameworkSerializer;
import com.netflix.zeno.hash.HashSerializationFramework;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * Mirroring the FrameworkSerializer, FrameworkDeserializer will define the actions which must be taken to decode individual "Zeno native" elements during deserialization
 * of objects.  It will also define the method of traversal of serialized representations of Objects for deserialization
 * via the specification of deserializeObject, deserializeList, deserializeSet, and deserializeMap methods.<p/>
 *
 * There may or may not be a "deserialization", which corresponds to every "serialization".  For example, one operation which ships with
 * the zeno framework is a calculation of a hash of a given Object (see {@link HashSerializationFramework}).  This is a one-way operation and therefore has no corresponding
 * "deserialization".  In this case, the {@link HashFrameworkSerializer} does not have a corresponding deserializer.
 *
 * @author dkoszewnik
 *
 * @param <D>
 */
public abstract class FrameworkDeserializer <D extends NFDeserializationRecord> {

    protected final SerializationFramework framework;

    protected FrameworkDeserializer(SerializationFramework framework){
        this.framework = framework;
    }

    /**
     * Deserializing java boolean
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public Boolean deserializeBoolean(D rec, String fieldName);

    /**
     * Can be overridden to avoid boxing an int where appropriate
     */
    public boolean deserializePrimitiveBoolean(D rec, String fieldName) {
        return deserializeBoolean(rec, fieldName).booleanValue();
    }

    /**
     * Deserializing java integer
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public Integer deserializeInteger(D rec, String fieldName);

    /**
     * Can be overridden to avoid boxing an int where appropriate
     */
    public int deserializePrimitiveInt(D rec, String fieldName) {
        return deserializeInteger(rec, fieldName).intValue();
    }

    /**
     * Deserializing java long
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public Long deserializeLong(D rec, String fieldName);

    /**
     * Can be overridden to avoid boxing a long where appropriate
     */
    public long deserializePrimitiveLong(D rec, String fieldName) {
        return deserializeLong(rec, fieldName).longValue();
    }

    /**
     * Deserializing java float
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public Float deserializeFloat(D rec, String fieldName);

    /**
     * Can be overridden to avoid boxing a float where appropriate
     */
    public float deserializePrimitiveFloat(D rec, String fieldName) {
        return deserializeFloat(rec, fieldName).floatValue();
    }

    /**
     * Deserializing java double
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public Double deserializeDouble(D rec, String fieldName);

    /**
     * Can be overridden to avoid boxing a double where appropriate
     */
    public double deserializePrimitiveDouble(D rec, String fieldName) {
        return deserializeDouble(rec, fieldName).doubleValue();
    }


    /**
     * Deserializing java string
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public String deserializeString(D rec, String fieldName);

    /**
     * Deserializing byte array
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public byte[] deserializeBytes(D rec, String fieldName);

    /**
     * @Deprecated instead use deserializeObject(D rec, String fieldName, Class<T> clazz)
     * 
     * Deserializing class object
     * @param rec
     * @param fieldName
     * @param typeName
     * @param clazz
     * @param obj
     */
    @Deprecated     
    abstract public <T> T deserializeObject(D rec, String fieldName, String typeName, Class<T> clazz);

    /**
     * Deserializing class object
     * @param rec
     * @param fieldName
     * @param clazz
     * @param obj
     */
    abstract public <T> T deserializeObject(D rec, String fieldName, Class<T> clazz);
    
    /**
     * Deserializing list
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    abstract public <T> List<T> deserializeList(D rec, String fieldName, NFTypeSerializer<T> itemSerializer);

    /**
     * Deserializing set
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    abstract public <T> Set<T> deserializeSet(D rec, String fieldName, NFTypeSerializer<T> itemSerializer);

    /**
     * Deserializing map
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    abstract public <K, V> Map<K, V> deserializeMap(D rec, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer);
    /**
     * Deserialize sorted map
     * @param rec
     * @param fieldName
     * @param keySerializer
     * @param valueSerializer
     */
    abstract public <K,V> SortedMap<K,V> deserializeSortedMap(D rec, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer);


}
