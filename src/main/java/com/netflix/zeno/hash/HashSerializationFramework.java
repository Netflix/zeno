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

import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 *
 * @author tvaliulin
 *
 */
public class HashSerializationFramework extends SerializationFramework
{
    public HashSerializationFramework(SerializerFactory factory) {
        super(factory);
        this.frameworkSerializer = new HashFrameworkSerializer(this);
    }

    public <T> byte[] getHash(String objectType, T object) {
        NFTypeSerializer<T> serializer = getSerializer(objectType);
        HashGenericRecord rec = new HashGenericRecord();
        serializer.serialize(object, rec);
        return rec.hash();
    }
}
