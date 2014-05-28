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
        //
        // {
        // List<MyClass> sample = new ArrayList<MyClass>();
        // int number = 100000;
        // for (int i = 0; i < number; i++) {
        // MyClass i1 = new MyClass();
        // map.put(i1, number + i, 2 * number + i);
        // sample.add(i1);
        // }
        // Assert.assertEquals(number, map.size());
        // sample.clear();
        // sample = null;
        // }
        //
        // Thread.sleep(5000);
        // System.gc();
        //
        // Assert.assertEquals(0, map.size());

        MyClass i1 = new MyClass();
        MyClass i2 = new MyClass();
        MyClass i3 = new MyClass();
        map.put(i1, 1, 4);
        map.put(i2, 2, 5);
        map.put(i3, 3, 6);

        Assert.assertEquals(3, map.size());
        map.clear();
        Assert.assertEquals(0, map.size());

        map.put(i1, 1, 4);
        map.put(i2, 2, 5);
        map.put(i3, 3, 6);

        WeakObjectOrdinalMap.Entry entry1 = map.getEntry(i1);
        Assert.assertEquals(1, entry1.getOrdinal());
        Assert.assertEquals(4, entry1.getImageMembershipsFlags());
        WeakObjectOrdinalMap.Entry entry2 = map.getEntry(i2);
        Assert.assertEquals(2, entry2.getOrdinal());
        Assert.assertEquals(5, entry2.getImageMembershipsFlags());
        WeakObjectOrdinalMap.Entry entry3 = map.getEntry(i3);
        Assert.assertEquals(3, entry3.getOrdinal());
        Assert.assertEquals(6, entry3.getImageMembershipsFlags());
        Assert.assertEquals(3, map.size());

        i2 = null;
        i3 = null;
        doGC();
        Assert.assertEquals(1, map.size());
        entry1 = map.getEntry(i1);
        Assert.assertEquals(1, entry1.getOrdinal());
        Assert.assertEquals(4, entry1.getImageMembershipsFlags());

        i1 = null;
        doGC();
        Assert.assertEquals(0, map.size());
    }

    private void doGC() throws InterruptedException {
        System.gc();
        Thread.sleep(1000);
    }
}
