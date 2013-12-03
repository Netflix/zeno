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

import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * This framework is used to create a GenericObject representation of data.
 *
 * The GenericObject representation is used by the diff HTML generator.
 *
 * @author dkoszewnik
 *
 */
public class GenericObjectSerializationFramework extends SerializationFramework {

    public GenericObjectSerializationFramework(SerializerFactory factory) {
        super(factory);
        this.frameworkSerializer = new GenericObjectFrameworkSerializer(this);
    }

    public GenericObject serialize(Object obj, String serializerName) {
        GenericObject diffObject = new GenericObject(serializerName, obj);

        getSerializer(serializerName).serialize(obj, diffObject);

        return diffObject;
    }


}
