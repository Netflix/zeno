package com.netflix.zeno.util.collections.impl;

import com.netflix.zeno.util.collections.builder.MapBuilder;

import java.util.Iterator;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class OpenAddressHashMapTest {

    @Test
    public void putAndRetrieve() {
        MapBuilder<Integer, Integer> builder = new OpenAddressingHashMap<Integer, Integer>();
        builder.builderInit(5);

        builder.builderPut(0, 1, 2);
        builder.builderPut(1, 3, 4);
        builder.builderPut(2, 5, 6);
        builder.builderPut(3, 7, 8);
        builder.builderPut(4, 9, 0);

        Map<Integer, Integer> map = builder.builderFinish();

        Assert.assertEquals(Integer.valueOf(2), map.get(1));
        Assert.assertEquals(Integer.valueOf(4), map.get(3));
        Assert.assertEquals(Integer.valueOf(6), map.get(5));
        Assert.assertEquals(Integer.valueOf(8), map.get(7));
        Assert.assertEquals(Integer.valueOf(0), map.get(9));

        Assert.assertEquals(5, map.size());
    }

    @Test
    public void putAndRetrieveWithNullValues() {
        MapBuilder<Integer, Integer> builder = new OpenAddressingHashMap<Integer, Integer>();
        builder.builderInit(5);

        builder.builderPut(0, 1, 2);
        builder.builderPut(1, 3, 4);
        builder.builderPut(2, 5, null);
        builder.builderPut(3, 7, 8);
        builder.builderPut(4, 9, 0);

        Map<Integer, Integer> map = builder.builderFinish();

        Assert.assertEquals(Integer.valueOf(2), map.get(1));
        Assert.assertEquals(Integer.valueOf(4), map.get(3));
        Assert.assertEquals(null, map.get(5));
        Assert.assertEquals(Integer.valueOf(8), map.get(7));
        Assert.assertEquals(Integer.valueOf(0), map.get(9));

        Assert.assertEquals(5, map.size());
    }


    @Test
    public void putAndRetrieveWithNullKeys() {
        MapBuilder<Integer, Integer> builder = new OpenAddressingHashMap<Integer, Integer>();
        builder.builderInit(5);

        builder.builderPut(0, 1, 2);
        builder.builderPut(1, 3, 4);
        builder.builderPut(2, null, 6);
        builder.builderPut(3, 7, 8);
        builder.builderPut(4, 9, 0);

        Map<Integer, Integer> map = builder.builderFinish();

        Assert.assertEquals(Integer.valueOf(2), map.get(1));
        Assert.assertEquals(Integer.valueOf(4), map.get(3));
        Assert.assertEquals(Integer.valueOf(6), map.get(null));
        Assert.assertEquals(Integer.valueOf(8), map.get(7));
        Assert.assertEquals(Integer.valueOf(0), map.get(9));

        Assert.assertEquals(5, map.size());
    }

    @Test
    public void sizeIsCorrectBasedOnNumberOfItemsAdded() {
        MapBuilder<Integer, Integer> builder = new OpenAddressingHashMap<Integer, Integer>();
        builder.builderInit(10);

        builder.builderPut(0, 1, 2);
        builder.builderPut(1, 3, 4);
        builder.builderPut(2, null, 6);
        builder.builderPut(3, 7, 8);
        builder.builderPut(4, 9, 0);

        Map<Integer, Integer> map = builder.builderFinish();

        Assert.assertEquals(Integer.valueOf(2), map.get(1));
        Assert.assertEquals(Integer.valueOf(4), map.get(3));
        Assert.assertEquals(Integer.valueOf(6), map.get(null));
        Assert.assertEquals(Integer.valueOf(8), map.get(7));
        Assert.assertEquals(Integer.valueOf(0), map.get(9));

        Assert.assertEquals(5, map.size());

    }

    @Test
    public void sortedHashMapIteratesSortedEntries() {
        MapBuilder<Integer, Integer> builder = new OpenAddressingSortedHashMap<Integer, Integer>();
        builder.builderInit(10);

        builder.builderPut(0, 7, 8);
        builder.builderPut(1, 9, 0);
        builder.builderPut(2, 3, 4);
        builder.builderPut(3, 1, 2);
        builder.builderPut(4, null, 6);

        Map<Integer, Integer> map = builder.builderFinish();

        Assert.assertEquals(Integer.valueOf(2), map.get(1));
        Assert.assertEquals(Integer.valueOf(4), map.get(3));
        Assert.assertEquals(Integer.valueOf(6), map.get(null));
        Assert.assertEquals(Integer.valueOf(8), map.get(7));
        Assert.assertEquals(Integer.valueOf(0), map.get(9));

        Assert.assertEquals(5, map.size());

        Iterator<Map.Entry<Integer, Integer>> entryIter = map.entrySet().iterator();

        Assert.assertTrue(entryIter.hasNext());
        Map.Entry<Integer, Integer> curEntry = entryIter.next();
        Assert.assertEquals(null, curEntry.getKey());
        Assert.assertEquals(Integer.valueOf(6), curEntry.getValue());

        Assert.assertTrue(entryIter.hasNext());
        curEntry = entryIter.next();
        Assert.assertEquals(Integer.valueOf(1), curEntry.getKey());
        Assert.assertEquals(Integer.valueOf(2), curEntry.getValue());

        Assert.assertTrue(entryIter.hasNext());
        curEntry = entryIter.next();
        Assert.assertEquals(Integer.valueOf(3), curEntry.getKey());
        Assert.assertEquals(Integer.valueOf(4), curEntry.getValue());

        Assert.assertTrue(entryIter.hasNext());
        curEntry = entryIter.next();
        Assert.assertEquals(Integer.valueOf(7), curEntry.getKey());
        Assert.assertEquals(Integer.valueOf(8), curEntry.getValue());

        Assert.assertTrue(entryIter.hasNext());
        curEntry = entryIter.next();
        Assert.assertEquals(Integer.valueOf(9), curEntry.getKey());
        Assert.assertEquals(Integer.valueOf(0), curEntry.getValue());

        Assert.assertFalse(entryIter.hasNext());

    }


}
