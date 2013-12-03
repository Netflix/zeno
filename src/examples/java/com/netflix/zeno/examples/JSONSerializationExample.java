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
package com.netflix.zeno.examples;

import com.netflix.zeno.examples.pojos.A;
import com.netflix.zeno.examples.pojos.B;
import com.netflix.zeno.examples.pojos.C;
import com.netflix.zeno.examples.serializers.ExampleSerializerFactory;
import com.netflix.zeno.json.JsonSerializationFramework;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * This is an example usage of the JsonSerializationframework.<p/>
 * 
 * Usage is detailed in the <a href="https://github.com/Netflix/zeno/wiki">documentation</a> 
 * on the page <a href="https://github.com/Netflix/zeno/wiki/Creating-json-data">creating json data</a>.
 * 
 * @author dkoszewnik
 *
 */
public class JSONSerializationExample {

    @Test
    @SuppressWarnings("unused")
    public void serializeJson() throws IOException {

        JsonSerializationFramework jsonFramework = new JsonSerializationFramework(new ExampleSerializerFactory());

        String json = jsonFramework.serializeAsJson("A", getExampleA());

        System.out.println("JSON FOR A:");
        System.out.println(json);

        A deserializedA = jsonFramework.deserializeJson("A", json);

        String bJson = jsonFramework.serializeAsJson("B", new B(50, "fifty"));

        System.out.println("JSON FOR B:");
        System.out.println(bJson);

    }

    public A getExampleA() {
        B b1 = new B(100, "one hundred");
        B b2 = new B(2000, "two thousand");

        List<B> bList = new ArrayList<B>();
        bList.add(b1);
        bList.add(b2);

        C c = new C(Long.MAX_VALUE, new byte[] { 1, 2, 3, 4, 5 });

        A a = new A(bList, c, 1);

        return a;
    }


}
