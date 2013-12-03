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

import com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyDerivableKeyHashMap;
import com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyMapArrayRecycler;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HeapFriendlyDerivableKeyHashMapTest {

    private HeapFriendlyMapArrayRecycler recycler;

    @Before
    public void setUp() {
        recycler = HeapFriendlyMapArrayRecycler.get();
        recycler.clear();
    }

    @After
    public void tearDown() {
        recycler.clear();
    }

    @Test
    public void setAndGet() {
        Map<String, Integer> map = getMap(30000);

        for(int i=0;i<30000;i++) {
            Assert.assertTrue(map.containsKey(String.valueOf(i)));
            Integer val = map.get(String.valueOf(i));
            Assert.assertEquals(i, val.intValue());
        }

        for(int i=30000;i<60000;i++) {
            Assert.assertFalse(map.containsKey(String.valueOf(i)));
            Assert.assertNull(map.get(String.valueOf(i)));
        }
    }

    @Test
    public void putWithSameDerivableKeyReplacesExistingEntry() {
        HeapFriendlyDerivableKeyHashMap<String, Integer> map = new HeapFriendlyDerivableKeyHashMap<String, Integer>(1) {
            protected String deriveKey(Integer value) {
                return String.valueOf(value);
            }
        };

        Integer one1 = new Integer(1);
        Integer one2 = new Integer(1);

        map.put(one1);

        Assert.assertSame(one1, map.get("1"));

        map.put(one2);

        Assert.assertNotSame(one1, map.get("1"));
        Assert.assertSame(one2, map.get("1"));
    }

    @Test
    public void testEntrySet() {
        HeapFriendlyDerivableKeyHashMap<String, Integer> map = getMap(2500);

        Set<Integer> allValues = new HashSet<Integer>();

        for(Map.Entry<String, Integer> entry : map.entrySet()) {
            Assert.assertEquals(String.valueOf(entry.getValue()), entry.getKey());
            allValues.add(entry.getValue());
        }

        for(int i=0;i<2500;i++) {
            Assert.assertTrue(allValues.contains(Integer.valueOf(i)));
        }
    }

    @Test
    public void testKeySet() {
        HeapFriendlyDerivableKeyHashMap<String, Integer> map = getMap(2500);

        Set<String> allKeys = new HashSet<String>();

        Set<String> keySet = map.keySet();

        for(String key : keySet) {
            Assert.assertTrue(keySet.contains(key));
            allKeys.add(key);
        }

        for(int i=0;i<2500;i++) {
            Assert.assertTrue(keySet.contains(String.valueOf(i)));
            Assert.assertTrue(allKeys.contains(String.valueOf(i)));
        }
    }

    @Test
    public void testValueSet() {
        HeapFriendlyDerivableKeyHashMap<String, Integer> map = getMap(2500);

        Set<Integer> allKeys = new HashSet<Integer>();

        Collection<Integer> values = map.values();

        for(Integer value : values) {
            Assert.assertTrue(values.contains(value));
            allKeys.add(value);
        }

        for(int i=0;i<2500;i++) {
            Assert.assertTrue(values.contains(Integer.valueOf(i)));
            Assert.assertTrue(allKeys.contains(Integer.valueOf(i)));
        }
    }

    @Test
    public void recyclesObjectArraysFromAlternatingCycles() throws Exception {
        HeapFriendlyMapArrayRecycler recycler = HeapFriendlyMapArrayRecycler.get();

        HeapFriendlyDerivableKeyHashMap<String, Integer> map = getMap(100);
        Object[] firstSegment = getFirstSegment(map);

        recycler.swapCycleObjectArrays();

        map.releaseObjectArrays();
        recycler.clearNextCycleObjectArrays();
        map = getMap(100);
        Object[] differentFirstSegment = getFirstSegment(map);

        Assert.assertNotSame(firstSegment, differentFirstSegment);

        recycler.swapCycleObjectArrays();

        map.releaseObjectArrays();
        recycler.clearNextCycleObjectArrays();
        map = getMap(100);
        Object[] firstSegmentAgain = getFirstSegment(map);

        Assert.assertSame(firstSegment, firstSegmentAgain);
    }

    @Test
    public void sizeIsCorrectAfterReplacement() {
        HeapFriendlyDerivableKeyHashMap<String, String> map = new HeapFriendlyDerivableKeyHashMap<String, String>(2) {
            protected String deriveKey(String value) {
                return value;
            }
        };

        map.put("a");
        map.put("b");
        map.put("a");

        Assert.assertEquals(2, map.size());
    }

    private Object[] getFirstSegment(HeapFriendlyDerivableKeyHashMap<String, Integer> map) throws Exception {
        Field f = HeapFriendlyDerivableKeyHashMap.class.getDeclaredField("values");
        f.setAccessible(true);
        return ((Object[][])f.get(map))[0];
    }

    private HeapFriendlyDerivableKeyHashMap<String, Integer> getMap(int numEntries) {
        HeapFriendlyDerivableKeyHashMap<String, Integer> map = new HeapFriendlyDerivableKeyHashMap<String, Integer>(numEntries) {
            protected String deriveKey(Integer value) {
                return String.valueOf(value);
            }
        };

        for(int i=0;i<numEntries;i++) {
            Integer value = Integer.valueOf(i);
            map.put(value);
        }

        return map;
    }

}
