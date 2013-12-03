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
package com.netflix.zeno.diff;

import com.netflix.zeno.genericobject.DiffHtmlGenerator;
import com.netflix.zeno.genericobject.GenericObject;
import com.netflix.zeno.genericobject.GenericObjectSerializationFramework;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class DiffHtmlGeneratorTest {

    TypeA a1;
    TypeA a2;

    @Before
    public void setUp() {
        a1 = new TypeA(new TypeB(999, 888, 777), new TypeB(1, 2, 3), new TypeB(4, 5, 6));
        a2 = new TypeA(new TypeB(4, 5, 6), new TypeB(1, 92, 3), new TypeB(40, 41, 42));
    }

    @Test
    public void test() {
        SerializerFactory factory = new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new ASerializer() };
            }
        };

        DiffHtmlGenerator htmlGenerator = new DiffHtmlGenerator(factory);

        GenericObjectSerializationFramework framework = new GenericObjectSerializationFramework(factory);

        GenericObject obj1 = framework.serialize(a1, "TypeA");
        GenericObject obj2 = framework.serialize(a2, "TypeA");

        String diffHtml = htmlGenerator.generateDiff(obj1, obj2);

        /// 4, 5, 6 pair should be matched most closely and come first:
        int indexOf456Pair = diffHtml.indexOf("val1: 4");
        /// 1, 2, 3 and 1, 92, 3 should be matched second closest and come after
        int indexOf123Pair = diffHtml.indexOf("val1: 1");
        ///999, 888, 777 has no matches and will be shown in the right column only
        int indexOf999Element = diffHtml.indexOf("val1: 999");
        /// 40, 41, 42 has no matches
        int indexOf40Element = diffHtml.indexOf("val1: 40");

        Assert.assertTrue(indexOf456Pair < indexOf123Pair);
        Assert.assertTrue(indexOf123Pair < indexOf999Element);
        Assert.assertTrue(indexOf123Pair < indexOf40Element);
    }

}
