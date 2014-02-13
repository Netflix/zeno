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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * 
 * A framework serializer allows for the definition of operations to be performed during traversal of POJOs in order to accomplish
 * some task without requiring knowledge of the structure or semantics of the data.<p/>
 * 
 * For each "Zeno native" element type, some action can be defined.<p/>
 *
 * The FrameworkSerializer will define the method of traversal of the Objects during serialization <i>from</i> POJOs via the specification of
 * the serializeObject, serializeList, serializeSet, and serializeMap interfaces.<p/>
 *
 * @author dkoszewnik
 * @author tvaliulin
 *
 */
public abstract class FrameworkSerializer<S extends NFSerializationRecord> {

    protected final SerializationFramework framework;

    protected FrameworkSerializer(SerializationFramework framework){
        this.framework = framework;
    }

    @SuppressWarnings("rawtypes")
    public NFTypeSerializer getSerializer(String typeName){
        NFTypeSerializer serializer = framework.getSerializer(typeName);

        if( serializer == null){
            throw new RuntimeException("Serializer " + typeName + " is not found");
        }

        return serializer;
    }

    /**
     * Serializing java primitive
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public void serializePrimitive(S rec, String fieldName, Object value);

    /**
     * Can be overridden to avoid boxing an int where appropriate
     */
    public void serializePrimitive(S rec, String fieldName, int value) {
        serializePrimitive(rec, fieldName, Integer.valueOf(value));
    }

    /**
     * Can be overridden to avoid boxing a long where appropriate
     */
    public void serializePrimitive(S rec, String fieldName, long value) {
        serializePrimitive(rec, fieldName, Long.valueOf(value));
    }

    /**
     * Can be overridden to avoid boxing a float where appropriate
     */
    public void serializePrimitive(S rec, String fieldName, float value) {
        serializePrimitive(rec, fieldName, Float.valueOf(value));
    }

    /**
     * Can be overridden to avoid boxing a double where appropriate
     */
    public void serializePrimitive(S rec, String fieldName, double value) {
        serializePrimitive(rec, fieldName, Double.valueOf(value));
    }

    /**
     * Can be overridden to avoid boxing an int where appropriate
     */
    public void serializePrimitive(S rec, String fieldName, boolean value) {
        serializePrimitive(rec, fieldName, Boolean.valueOf(value));
    }


    /**
     * Serializing java array
     * @param rec
     * @param fieldName
     * @param value
     */
    abstract public void serializeBytes(S rec, String fieldName, byte[] value);

    /**
     * @deprecated instead use serializeObject(S rec, String fieldName, Object obj)
     * 
     * Serializing class object
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    @Deprecated
    abstract public void serializeObject(S rec, String fieldName, String typeName, Object obj);
    
    /**
     * Serializing class object
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    abstract public void serializeObject(S rec, String fieldName, Object obj);

    /**
     * Serializing list
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    abstract public <T> void serializeList(S rec, String fieldName, String typeName, Collection<T> obj);

    /**
     * Serializing set
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    abstract public <T> void serializeSet(S rec, String fieldName, String typeName, Set<T> obj);

    /**
     * Serializing map
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    abstract public <K, V> void serializeMap(S rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> obj);

    /**
     * Serialize sorted map
     * @param rec
     * @param fieldName
     * @param typeName
     * @param obj
     */
    public <K, V> void serializeSortedMap(S rec, String fieldName, String keyTypeName, String valueTypeName, SortedMap<K, V> obj) {
        serializeMap(rec, fieldName, keyTypeName, valueTypeName, obj);
    }

}
