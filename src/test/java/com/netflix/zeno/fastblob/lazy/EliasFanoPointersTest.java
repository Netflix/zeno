package com.netflix.zeno.fastblob.lazy;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import org.junit.Assert;

public class EliasFanoPointersTest {

    Random rand = new Random();

    @Test
    public void randomizedTest() {
        int numValues = rand.nextInt(500000) + 1000000;
        int maxValue = rand.nextInt(10000000) + 2000000;

        int values[] = new int[numValues];

        for(int i=0;i<numValues;i++) {
            values[i] = rand.nextInt(maxValue);
        }

        Arrays.sort(values);

        values[0] = 0;

        EliasFanoPointers pointers = new EliasFanoPointers(maxValue, numValues);

        for(int i=0;i<numValues;i++) {
            if(i > 0 && values[i] == values[i-1])
                pointers.add(-1);
            else
                pointers.add(values[i]);
        }

        for(int i=0;i<numValues;i++) {
            if(i > 0 && values[i] == values[i - 1]) {
                Assert.assertEquals(-1, pointers.get(i));
            } else {
                Assert.assertEquals(values[i], pointers.get(i));
            }
        }
    }

    @Test
    public void testNullFirstPointer() {
        int numValues = 10;
        int maxValue = 100;

        EliasFanoPointers pointers = new EliasFanoPointers(maxValue, numValues);

        pointers.add(-1);
        pointers.add(20);

        Assert.assertEquals(-1, pointers.get(0));
        Assert.assertEquals(20, pointers.get(1));
    }

    @Test
    public void testZeroFirstPointer() {
        int numValues = 10;
        int maxValue = 100;

        EliasFanoPointers pointers = new EliasFanoPointers(maxValue, numValues);

        pointers.add(0);
        pointers.add(20);

        Assert.assertEquals(0, pointers.get(0));
        Assert.assertEquals(20, pointers.get(1));
    }

    @Test
    public void testNonzeroFirstPointer() {
        int numValues = 10;
        int maxValue = 100;

        EliasFanoPointers pointers = new EliasFanoPointers(maxValue, numValues);

        pointers.add(3);
        pointers.add(20);

        Assert.assertEquals(3, pointers.get(0));
        Assert.assertEquals(20, pointers.get(1));
    }


    @Test
    public void testLowBits() {
        EliasFanoPointers pointers = new EliasFanoPointers(100000, 10000);

        pointers.setLowBits(0, 4);
        pointers.setLowBits(21, 7);

        Assert.assertEquals(4, pointers.getLowBits(0));
        Assert.assertEquals(7, pointers.getLowBits(21));
    }

}
