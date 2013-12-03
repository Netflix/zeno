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
package com.netflix.zeno.fastblob;

import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.testpojos.TypeASerializer;
import com.netflix.zeno.testpojos.TypeBSerializer;
import com.netflix.zeno.testpojos.TypeCSerializer;
import com.netflix.zeno.testpojos.TypeDSerializer;
import com.netflix.zeno.testpojos.TypeESerializer;

public abstract class BlobSerializationGenericFrameworkAbstract extends BlobSerializationAbstract {

    protected SerializerFactory serializersFactory = new SerializerFactory(){
        @Override
        public NFTypeSerializer<?>[] createSerializers() {
            return new NFTypeSerializer<?>[] { new TypeASerializer(),
                    new TypeBSerializer(), new TypeCSerializer(),
                    new TypeDSerializer(), new TypeESerializer() };
        }
    };

    protected TypeASerializer typeASerializer = new TypeASerializer();
    protected TypeBSerializer TypeBSerializer = new TypeBSerializer();
    protected TypeCSerializer typeCSerializer = new TypeCSerializer();
    protected TypeDSerializer TypeDSerializer = new TypeDSerializer();
    protected TypeESerializer TypeESerializer = new TypeESerializer();

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

}
