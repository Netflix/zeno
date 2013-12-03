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
package com.netflix.zeno.examples.serializers;

import com.netflix.zeno.examples.pojos.A;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * An example SerializerFactory, which references the following types (classes / serializers):<p/>
 *
 * <ul>
 * <li>A / ASerializer</li>
 * <li>B / BSerializer</li>
 * <li>C / CSerializer</li>
 * </ul>
 *
 */
public class ExampleSerializerFactory implements SerializerFactory {

    public static final SerializerFactory INSTANCE = new ExampleSerializerFactory();

    @Override
    public NFTypeSerializer<?>[] createSerializers() {
        NFTypeSerializer<A> serializer = new ASerializer();

        // only ASerializer needs to be passed in the array.
        // CSerializer and DSerializer will be available because they are referenced by the
        // requiredSubSerializers() method of ASerializer.
        return new NFTypeSerializer<?>[] { serializer };
    }

}
