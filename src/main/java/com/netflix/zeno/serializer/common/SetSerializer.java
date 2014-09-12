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

import java.util.Set;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;

/**
*
* A default NFTypeSerializer implementation for Set objects.
*
*/
public class SetSerializer<E> extends CollectionSerializer<E, Set<E>> {

    public SetSerializer(String schemaName, NFTypeSerializer<E> elementSerializer) {
        super(schemaName, elementSerializer);
    }

    public SetSerializer(NFTypeSerializer<E> elementSerializer) {
        this("SetOf" + elementSerializer.getName(), elementSerializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void doSerialize(Set<E> list, NFSerializationRecord rec) {
        serializationFramework.getFrameworkSerializer().serializeSet(rec, ORDINALS_FIELD_NAME, elementSerializer.getName(), list);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Set<E> doDeserialize(NFDeserializationRecord rec) {
        return serializationFramework.getFrameworkDeserializer().deserializeSet(rec, ORDINALS_FIELD_NAME, elementSerializer);
    }


    @Override
    public void setSerializationFramework(SerializationFramework framework) {
        this.serializationFramework = framework;
        this.elementSerializer.setSerializationFramework(framework);
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(setField(ORDINALS_FIELD_NAME, elementSerializer.getName()));
    }

}
