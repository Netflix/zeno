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
package com.netflix.zeno.examples.framework;

import com.netflix.zeno.examples.pojos.A;
import com.netflix.zeno.examples.pojos.B;
import com.netflix.zeno.examples.pojos.C;
import com.netflix.zeno.examples.serializers.ExampleSerializerFactory;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.junit.Assert;

/**
 * Example usage of the {@link IntSumFramework}.
 * 
 * @author dkoszewnik
 *
 */
public class IntSumFrameworkExample {

    @Test
    public void determineSumOfValues() {
        IntSumFramework sumFramework = new IntSumFramework(new ExampleSerializerFactory());

        B b1 = new B(12, "Twelve!");  /// sum = 12
        B b2 = new B(25, "Plus Twenty Five!");  /// sum = 37
        B b3 = new B(10, "Plus Ten!");  /// sum = 47

        List<B> bList = Arrays.asList(b1, b2, b3);

        C c = new C(20, new byte[] { 100, 101, 102 });  /// longs don't count.  Still sum = 47.

        A a = new A(bList, c, 100); /// sum = 147

        int actualSum = sumFramework.getSum("A", a);

        Assert.assertEquals(147, actualSum);
    }

}
