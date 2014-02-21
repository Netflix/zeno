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

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.Collection;
import java.util.Collections;

/**
*
* A default NFTypeSerializer implementation for Enums.<p/>
*
* NOTE: Including enum types in your data model may cause issues.  The enum is serialized as a String, which is the Enum name.
* If the Enum does not exist during deserialization, null will be returned during deserialization.
*
*/
@SuppressWarnings("rawtypes")
public class EnumSerializer<T extends Enum> extends NFTypeSerializer<T> {

    private final Class<T> enumClazz;

    public EnumSerializer(Class<T> enumClazz) {
        super(unqualifiedClassName(enumClazz));
        this.enumClazz = enumClazz;
    }

    @Override
    public void doSerialize(T value, NFSerializationRecord rec) {
        serializePrimitive(rec, "value", value == null ? null : value.name());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected T doDeserialize(NFDeserializationRecord rec) {
        String value = deserializePrimitiveString(rec, "value");
        try {
            return (T) Enum.valueOf(enumClazz, value);
        } catch (Exception ignore) { }
        return null;
    }

    @Override
    protected FastBlobSchema createSchema() {
        return schema(
                field("value", FieldType.STRING)
        );
    }

    @Override
    public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
        return Collections.emptyList();
    }

    private static String unqualifiedClassName(Class<?>clazz) {
        String qualifiedName = clazz.getCanonicalName();
        if(qualifiedName.indexOf('.') < 0)
            return qualifiedName;
        return qualifiedName.substring(qualifiedName.lastIndexOf('.') + 1);
    }

}
