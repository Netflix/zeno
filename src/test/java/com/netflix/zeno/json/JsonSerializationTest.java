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
import com.netflix.zeno.serializer.common.IntegerSerializer;
import com.netflix.zeno.testpojos.TypeA;
import com.netflix.zeno.testpojos.TypeASerializer;
import com.netflix.zeno.testpojos.TypeB;
import com.netflix.zeno.testpojos.TypeC;
import com.netflix.zeno.testpojos.TypeCSerializer;
import com.netflix.zeno.testpojos.TypeD;
import com.netflix.zeno.testpojos.TypeG;
import com.netflix.zeno.testpojos.TypeGSerializer;

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
    private SerializerFactory typeGSerializerFactory;

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

        typeGSerializerFactory = new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeGSerializer() };
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

        TypeC deserializedTypeC = jsonFramework.deserializeJson("TypeC", json);

        Assert.assertEquals(originalTypeC, deserializedTypeC);
    }

    @Test
    public void roundTripJsonMap() throws IOException {
        Map<Integer, TypeA> map = new HashMap<Integer, TypeA>();
        map.put(1, new TypeA(0, 1));
        map.put(2, new TypeA(2, 3));

        JsonSerializationFramework jsonFramework = new JsonSerializationFramework(new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeASerializer(), new IntegerSerializer() };
            }
        });

        String json = jsonFramework.serializeJsonMap(IntegerSerializer.NAME, "TypeA", map, true);

        Map<Integer, TypeA> deserializedMap = jsonFramework.deserializeJsonMap(IntegerSerializer.NAME, "TypeA", json);

        Assert.assertEquals(2, deserializedMap.size());
        Assert.assertEquals(new TypeA(0, 1), deserializedMap.get(1));
        Assert.assertEquals(new TypeA(2, 3), deserializedMap.get(2));
    }

    @Test
    public void roundTripJsonWithTwoHierarchicalLevels() throws IOException {
        JsonSerializationFramework jsonFramework = new JsonSerializationFramework(typeGSerializerFactory);

        String json = jsonFramework.serializeAsJson("TypeG", new TypeG(new TypeD(1, new TypeA(2, 3))));

        try {
            jsonFramework.deserializeJson("TypeG", json);
        } catch(Exception e) {
            Assert.fail("Exception was thrown");
        }
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
