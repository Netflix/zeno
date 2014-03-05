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
package com.netflix.zeno.genericobject;

import com.netflix.zeno.genericobject.GenericObject.CollectionType;
import com.netflix.zeno.serializer.FrameworkSerializer;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.util.PrimitiveObjectIdentifier;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

/**
 * The GenericObject representation is used by the diff HTML generator.
 *
 * @author dkoszewnik
 *
 */
public class GenericObjectFrameworkSerializer extends FrameworkSerializer<GenericObject> {

    public GenericObjectFrameworkSerializer(SerializationFramework framework) {
        super(framework);
    }

    @Override
    public void serializePrimitive(GenericObject rec, String fieldName, Object value){
        if(value != null){
            if( value.getClass().isEnum()){
                value = ((Enum<?>)value).name();
            } else if(value instanceof Date) {
                value = value.toString();
            }
        }
        rec.add(fieldName, value);
    }

    @Override
    public void serializeBytes(GenericObject rec, String fieldName, byte[] value) {
        String str = null;
        if(value != null){
            byte encoded[] = Base64.encodeBase64(value, true);
            try {
                str = new String(encoded, "UTF-8");
            } catch (UnsupportedEncodingException ignore) { }
        }
    	rec.add(fieldName, str);
    }



    private static boolean isPrimitive(Class<?> type){
        return type.isEnum() || PrimitiveObjectIdentifier.isPrimitiveOrWrapper(type);
    }

    /*
     * @Deprecated instead use serializeObject(GenericObject rec, String fieldName, Object obj)
     *
     */
    @Deprecated
    @Override
    @SuppressWarnings("unchecked")
    public void serializeObject(GenericObject rec, String fieldName, String typeName, Object obj) {
        if( obj == null ){
            rec.add(fieldName, null);
        } else if (isPrimitive(obj.getClass())){
            serializePrimitive(rec, fieldName, obj);
            return;
        } else {
            GenericObject subObject = new GenericObject(typeName, obj);
            getSerializer(typeName).serialize(obj, subObject);
            rec.add(fieldName, subObject);
        }
    }

    @Override
    public void serializeObject(GenericObject rec, String fieldName, Object obj) {
        serializeObject(rec, fieldName, rec.getObjectType(fieldName), obj);
    }

    @Override
    public <T> void serializeList(GenericObject rec, String fieldName, String elementTypeName, Collection<T> obj) {
        serializeCollection(rec, fieldName, "List", elementTypeName, obj);
    }

    @Override
    public <T> void serializeSet(GenericObject rec, String fieldName, String elementTypeName, Set<T> obj) {
        serializeCollection(rec, fieldName, "Set", elementTypeName, obj);
    }

    private <T> void serializeCollection(GenericObject rec, String fieldName, String collectionType, String elementTypeName, Collection<T> obj) {
        if(obj == null) {
            rec.add(fieldName, null);
        } else {
            GenericObject setObject = new GenericObject(collectionType, CollectionType.COLLECTION, obj);
            serializeCollectionElements(setObject, elementTypeName, obj);
            rec.add(fieldName, setObject);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void serializeCollectionElements(GenericObject record, String elementTypeName, Collection<T> obj) {
        int counter = 0;
        for(T element : obj) {
            if(element == null) {
                record.add("element", obj, ++counter);
            } else {
                NFTypeSerializer<Object> elementSerializer = getSerializer(elementTypeName);
                GenericObject elementObject = new GenericObject(elementTypeName, element);
                elementSerializer.serialize(element, elementObject);
                record.add("element", elementObject, ++counter);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void serializeMap(GenericObject record, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> map) {
        if(map == null) {
            record.add(fieldName, null);
        }

        GenericObject mapObject = new GenericObject("Map", CollectionType.MAP, map);
        int counter = 0;

        for(Map.Entry<K, V> entry : map.entrySet()) {
            counter++;
            GenericObject entryObject = new GenericObject("Map.Entry", entry);

            NFTypeSerializer<Object> keySerializer = getSerializer(keyTypeName);
            GenericObject keyObject = new GenericObject(keyTypeName, entry.getKey());
            keySerializer.serialize(entry.getKey(), keyObject);
            entryObject.add("key", keyObject);

            if(entry.getValue() == null) {
                entryObject.add("value", null);
            } else {
                NFTypeSerializer<Object> valueSerializer = getSerializer(valueTypeName);
                GenericObject valueObject = new GenericObject(valueTypeName, entry.getValue());
                valueSerializer.serialize(entry.getValue(), valueObject);
                entryObject.add("value", valueObject);
            }

            mapObject.add("entry", entryObject, counter);
        }

        record.add(fieldName, mapObject);
     }

}
