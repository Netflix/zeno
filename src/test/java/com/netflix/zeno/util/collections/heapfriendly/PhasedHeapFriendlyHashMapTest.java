/*
 *
 *  Copyright 2015 Netflix, Inc.
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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * JUnit tests for {@link PhasedHeapFriendlyHashMap}
 *
 * @author drbathgate
 *
 */
public class PhasedHeapFriendlyHashMapTest {

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
        PhasedHeapFriendlyHashMap<String, Integer> map = new PhasedHeapFriendlyHashMap<String, Integer>();

        Integer one1 = new Integer(1);
        Integer one2 = new Integer(1);

        map.beginDataSwapPhase(1);
        map.put("1", one1);
        map.endDataSwapPhase();

        Assert.assertSame(one1, map.get("1"));

        map.beginDataSwapPhase(1);
        map.put("1", one1);
        map.put("1", one2);
        map.endDataSwapPhase();

        Assert.assertNotSame(one1, map.get("1"));
        Assert.assertSame(one2, map.get("1"));
    }

    @Test
    public void testEntrySet() {
        PhasedHeapFriendlyHashMap<String, Integer> map = getMap(2500);

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
        PhasedHeapFriendlyHashMap<String, Integer> map = getMap(2500);

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
        PhasedHeapFriendlyHashMap<String, Integer> map = getMap(2500);

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
    public void testImproperPut() {
        boolean exceptionThrown = false;

        PhasedHeapFriendlyHashMap<String, Integer> map = new PhasedHeapFriendlyHashMap<String, Integer>();

        try {
            map.put("1", 1);
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);
    }

    @Test
    public void testImproperGet() {

        PhasedHeapFriendlyHashMap<String, Integer> map = new PhasedHeapFriendlyHashMap<String, Integer>();

        map.beginDataSwapPhase(1);
        map.put("1", 1);

        Assert.assertNull(map.get("1"));

        map.endDataSwapPhase();

        Assert.assertNotNull(map.get("1"));
    }

    @Test
    public void testImproperBeginSwapPhase() {
        boolean exceptionThrown = false;

        PhasedHeapFriendlyHashMap<String, Integer> map = new PhasedHeapFriendlyHashMap<String, Integer>();

        map.beginDataSwapPhase(1);

        try {
            map.beginDataSwapPhase(1);
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);
    }

    @Test
    public void testImproperEndSwapPhase() {
        boolean exceptionThrown = false;

        PhasedHeapFriendlyHashMap<String, Integer> map = new PhasedHeapFriendlyHashMap<String, Integer>();

        try {
            map.endDataSwapPhase();
        } catch (IllegalStateException e) {
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);
    }

    private PhasedHeapFriendlyHashMap<String, Integer> getMap(int numOfEntries){
        PhasedHeapFriendlyHashMap<String, Integer> map = new PhasedHeapFriendlyHashMap<String, Integer>();

        map.beginDataSwapPhase(numOfEntries);

        for (int i = 0; i < numOfEntries; i++) {
            map.put(String.valueOf(i), i);
        }

        map.endDataSwapPhase();

        return map;
    }
}
