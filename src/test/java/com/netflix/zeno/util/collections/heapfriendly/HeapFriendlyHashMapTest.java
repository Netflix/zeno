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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HeapFriendlyHashMapTest {

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
        HeapFriendlyHashMap<String, Integer> map = new HeapFriendlyHashMap<String, Integer>(1);

        Integer one1 = new Integer(1);
        Integer one2 = new Integer(1);

        map.put("1", one1);

        Assert.assertSame(one1, map.get("1"));

        map.put("1", one2);

        Assert.assertNotSame(one1, map.get("1"));
        Assert.assertSame(one2, map.get("1"));
    }

    @Test
    public void testEntrySet() {
        HeapFriendlyHashMap<String, Integer> map = getMap(2500);

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
        HeapFriendlyHashMap<String, Integer> map = getMap(2500);

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
        HeapFriendlyHashMap<String, Integer> map = getMap(2500);

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

        HeapFriendlyHashMap<String, Integer> map = getMap(100);
        Object[] firstKeySegment = getFirstSegment(map, "keys");
        Object[] firstValueSegment = getFirstSegment(map, "values");


        recycler.swapCycleObjectArrays();

        map.releaseObjectArrays();
        map = getMap(100);
        Object[] differentFirstKeySegment = getFirstSegment(map, "keys");
        Object[] differentFirstValueSegment = getFirstSegment(map, "values");

        /// neither arrays are recycled from the first cycle.
        Assert.assertNotSame(firstKeySegment, differentFirstKeySegment);
        Assert.assertNotSame(firstValueSegment, differentFirstValueSegment);
        Assert.assertNotSame(firstKeySegment, differentFirstValueSegment);
        Assert.assertNotSame(firstValueSegment, differentFirstKeySegment);

        recycler.swapCycleObjectArrays();

        map.releaseObjectArrays();
        map = getMap(100);
        Object[] firstKeySegmentAgain = getFirstSegment(map, "keys");
        Object[] firstValueSegmentAgain = getFirstSegment(map, "values");

        /// both arrays are recycled, but it doesn't matter whether which was initially for the keys and which was initially for the values.
        Assert.assertTrue(firstKeySegmentAgain == firstKeySegment || firstKeySegmentAgain == firstValueSegment);
        Assert.assertTrue(firstValueSegmentAgain == firstKeySegment || firstValueSegmentAgain == firstValueSegment);
    }

    @Test
    public void sizeIsCorrectAfterReplacement() {
        HeapFriendlyHashMap<String, String> map = new HeapFriendlyHashMap<String, String>(2);

        map.put("a", "b");
        map.put("b", "d");
        map.put("a", "c");

        Assert.assertEquals(2, map.size());
    }

    private Object[] getFirstSegment(HeapFriendlyHashMap<String, Integer> map, String fieldName) throws Exception {
        Field f = HeapFriendlyHashMap.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        return ((Object[][])f.get(map))[0];
    }

    private HeapFriendlyHashMap<String, Integer> getMap(int numEntries) {
        HeapFriendlyHashMap<String, Integer> map = new HeapFriendlyHashMap<String, Integer>(numEntries);

        for(int i=0;i<numEntries;i++) {
            map.put(String.valueOf(i), Integer.valueOf(i));
        }

        return map;
    }

}
