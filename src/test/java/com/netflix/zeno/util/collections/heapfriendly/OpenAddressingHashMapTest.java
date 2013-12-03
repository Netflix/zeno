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
package com.netflix.zeno.util.collections.heapfriendly;

import com.netflix.zeno.util.collections.builder.MapBuilder;
import com.netflix.zeno.util.collections.impl.OpenAddressingHashMap;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class OpenAddressingHashMapTest {

    @Test
    public void mustHandleNullElements() {
        MapBuilder<Integer, String> builder = new OpenAddressingHashMap<Integer, String>();

        builder.builderInit(3);

        builder.builderPut(0, 1, "1");
        builder.builderPut(1, null, "2");
        builder.builderPut(2, 3, "3");

        Map<Integer, String> map = builder.builderFinish();

        Assert.assertEquals(3, map.size());
        Assert.assertEquals("1", map.get(1));
        Assert.assertEquals("2", map.get(null));
        Assert.assertEquals("3", map.get(3));
    }

    @Test
    public void mustNotBreakWhenInitIsHintedWithTooManyElements() {
        MapBuilder<Integer, String> builder = new OpenAddressingHashMap<Integer, String>();

        builder.builderInit(4);

        builder.builderPut(0, 1, "1");
        builder.builderPut(1, 2, "2");

        Map<Integer, String> map = builder.builderFinish();

        Assert.assertEquals("1", map.get(1));
        Assert.assertEquals("2", map.get(2));
        Assert.assertEquals(null, map.get(null));
        //TODO: Fix the map.size();
        //Assert.assertEquals(2, map.size());
    }

}
