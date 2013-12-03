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

import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;

import java.util.List;


/**
*
* A default NFTypeSerializer implementation for List objects.
*
*/
public class ListSerializer<E> extends CollectionSerializer<E, List<E>> {

    public ListSerializer(String name, NFTypeSerializer<E> elementSerializer) {
        super(name, elementSerializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected List<E> doDeserialize(NFDeserializationRecord rec) {
        return serializationFramework.getFrameworkDeserializer().deserializeList(rec, ORDINALS_FIELD_NAME, elementSerializer);
    }

    @Override
    public void setSerializationFramework(SerializationFramework framework) {
        this.serializationFramework = framework;
        this.elementSerializer.setSerializationFramework(framework);
    }

}
