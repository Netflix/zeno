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

import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

/**
*
* A default NFTypeSerializer implementation for Collection objects.
*
* This is an abstract class, implemented by {@link ListSerializer} and {@link SetSerializer}
*
*/
public abstract class CollectionSerializer<E, T extends Collection<E>> extends NFTypeSerializer<T> {

    protected static final String ORDINALS_FIELD_NAME = "ordinals";

    protected final NFTypeSerializer<E> elementSerializer;

    @SuppressWarnings({ "unchecked" })
    @Override
    public void doSerialize(T list, NFSerializationRecord rec) {
        serializationFramework.getFrameworkSerializer().serializeList(rec, ORDINALS_FIELD_NAME, elementSerializer.getName(), list);
    }

    public CollectionSerializer(String schemaName, NFTypeSerializer<E> elementSerializer) {
        super(schemaName);
        this.elementSerializer = elementSerializer;
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return serializers(elementSerializer);
    }

}
