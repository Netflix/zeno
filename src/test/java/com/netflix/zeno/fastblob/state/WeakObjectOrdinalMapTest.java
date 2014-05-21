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
package com.netflix.zeno.fastblob.state;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

public class WeakObjectOrdinalMapTest {

    private static class MyClass {
        int i = RandomUtils.nextInt();
    }

    @Test
    public void map() throws Exception {
        WeakObjectOrdinalMap map = new WeakObjectOrdinalMap(8);

        MyClass i1 = new MyClass();
        MyClass i2 = new MyClass();
        MyClass i3 = new MyClass();
        map.put(i1, 1);
        map.put(i2, 2);
        map.put(i3, 3);
        Assert.assertEquals(1, map.get(i1));
        Assert.assertEquals(2, map.get(i2));
        Assert.assertEquals(3, map.get(i3));
        Assert.assertEquals(3, map.size());

        i2 = null;
        i3 = null;
        System.gc();
        Thread.sleep(5000);
        Assert.assertEquals(1, map.size());
        Assert.assertEquals(1, map.get(i1));

        i1 = null;
        System.gc();
        Thread.sleep(5000);
        Assert.assertEquals(0, map.size());
    }
}
