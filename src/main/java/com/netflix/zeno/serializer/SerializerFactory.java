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

/**
 * The interface that the using code should implement in order make serializers
 * available for frameworks to use.<p/>
 *
 * Each call to createSerializers() should return a unique array containing
 * unique instances of the top level serializers which define an Object model.
 * 
 * Check out the <a href="https://github.com/Netflix/zeno/wiki">Zeno documentation</a> section 
 * <a href="https://github.com/Netflix/zeno/wiki/Defining-an-object-model">defining an object model</a> for details about how
 * to define your data model, and then reference it with a SerializerFactory.
 *
 */
public interface SerializerFactory {
    /**
     * Creates the serializers
     *
     * @return
     */
    NFTypeSerializer<?>[] createSerializers();
}
