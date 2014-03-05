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

import com.netflix.zeno.hash.HashGenericRecordSerializers.Serializer;
import com.netflix.zeno.serializer.NFSerializationRecord;

/**
 *
 * @author tvaliulin
 *
 */
public final class HashGenericRecord extends NFSerializationRecord {
    HashAlgorithm hasher;

    public HashGenericRecord() {
        this(new HashOrderDependent());
    }

    public HashGenericRecord(HashAlgorithm hasher) {
        this.hasher = hasher;
    }

    public Object get(int arg0) {
        throw new UnsupportedOperationException();
    }

    public void put(int arg0, Object arg1) {
        write(arg0);
        write(arg1);
    }

    public void put(String arg0, Object arg1) {
        write(arg0);
        write(":");
        write(arg1);
    }

    private void write(Object obj) {
        try {
            if (obj == null) {
                hasher.write(0);
                return;
            }
            if (obj.getClass().isEnum()) {
                hasher.write(((Enum<?>) obj).name());
            } else if (obj.getClass().isArray()) {
                if (obj.getClass().getComponentType().isPrimitive()) {
                    Serializer serializer = HashGenericRecordSerializers.getPrimitiveArraySerializer(obj.getClass().getComponentType());
                    if (serializer == null) {
                        throw new RuntimeException("Can't find serializer for array of type:" + obj.getClass());
                    }
                    serializer.serialize(hasher, obj);
                } else {
                    Object[] objects = (Object[]) obj;
                    for (Object object : objects) {
                        write(object);
                    }
                }
            } else {
                Serializer serializer = HashGenericRecordSerializers.getTypeSerializer(obj.getClass());
                if (serializer == null) {
                    throw new RuntimeException("Can't find serializer for type:" + obj.getClass());
                }
                serializer.serialize(hasher, obj);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public byte[] hash() {
        return hasher.bytes();
    }

}
