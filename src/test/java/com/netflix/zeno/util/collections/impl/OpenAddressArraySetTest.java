package com.netflix.zeno.util.collections.impl;

import com.netflix.zeno.util.collections.builder.SetBuilder;

import java.util.Iterator;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class OpenAddressArraySetTest {

    @Test
    public void populateAndRetrieve() {
        SetBuilder<Integer> setBuilder = new OpenAddressingArraySet<Integer>();

        setBuilder.builderInit(15);

        setBuilder.builderSet(0, 0);
        setBuilder.builderSet(1, 1);
        setBuilder.builderSet(2, null);
        setBuilder.builderSet(3, 2);
        setBuilder.builderSet(4, 3);

        Set<Integer> set = setBuilder.builderFinish();

        Assert.assertEquals(5, set.size());
        Assert.assertTrue(set.contains(0));
        Assert.assertTrue(set.contains(3));
        Assert.assertTrue(set.contains(null));
        Assert.assertFalse(set.contains(4));

        Iterator<Integer> iter = set.iterator();

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(Integer.valueOf(0), iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(Integer.valueOf(1), iter.next());


        Assert.assertTrue(iter.hasNext());
        Assert.assertNull(iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(Integer.valueOf(2), iter.next());

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(Integer.valueOf(3), iter.next());

        Assert.assertFalse(iter.hasNext());
    }

}
