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
package com.netflix.zeno.json;

import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.testpojos.TypeA;
import com.netflix.zeno.testpojos.TypeASerializer;
import com.netflix.zeno.testpojos.TypeB;
import com.netflix.zeno.testpojos.TypeC;
import com.netflix.zeno.testpojos.TypeCSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JsonSerializationTest {

    private SerializerFactory typeCSerializerFactory;
    private SerializerFactory typeASerializerFactory;

    @Before
    public void setUp() {
        typeCSerializerFactory = new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeCSerializer() };
            }
        };

        typeASerializerFactory = new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeASerializer() };
            }
        };

    }

    private static final String expectedTypeAJson =
            "{\n" +
            "  \"val1\" : 1,\n" +
            "  \"val2\" : 2\n" +
            "}";

    @Test
    public void producesJson() {
        JsonSerializationFramework jsonFramework = new JsonSerializationFramework(typeASerializerFactory);

        String json = jsonFramework.serializeAsJson("TypeA", new TypeA(1, 2));

        Assert.assertEquals(expectedTypeAJson, json);
    }


    @Test
    public void consumesJson() throws IOException {
        JsonSerializationFramework jsonFramework = new JsonSerializationFramework(typeASerializerFactory);

        TypeA deserializedTypeA = jsonFramework.deserializeJson("TypeA", expectedTypeAJson);

        Assert.assertEquals(1, deserializedTypeA.getVal1());
        Assert.assertEquals(2, deserializedTypeA.getVal2());
    }

    @Test
    public void roundTripJson() throws IOException {
        TypeC originalTypeC = createTestTypeC();

        JsonSerializationFramework jsonFramework = new JsonSerializationFramework(typeCSerializerFactory);
        String json = jsonFramework.serializeAsJson("TypeC", createTestTypeC());

        System.out.println(json);

        TypeC deserializedTypeC = jsonFramework.deserializeJson("TypeC", json);

        Assert.assertEquals(originalTypeC, deserializedTypeC);
    }

    private TypeC createTestTypeC() {
        Map<String, TypeA> typeAMap = new HashMap<String, TypeA>();
        List<TypeB> typeBs = new ArrayList<TypeB>();

        typeAMap.put("a12", new TypeA(1, 2));
        typeAMap.put("a34", new TypeA(3, 4));

        typeBs.add(new TypeB(5, "five"));
        typeBs.add(new TypeB(6, "six"));

        return new TypeC(typeAMap, typeBs);
    }

}
