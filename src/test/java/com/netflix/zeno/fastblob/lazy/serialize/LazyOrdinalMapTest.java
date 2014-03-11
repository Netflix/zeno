package com.netflix.zeno.fastblob.lazy.serialize;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LazyOrdinalMapTest {

    private LazyOrdinalMap map;

    @Before
    public void setUp() {
        map = new LazyOrdinalMap();

        ByteDataBuffer buf = new ByteDataBuffer();

        for(int i=0;i<1000;i++) {
            buf.write((byte)(i >> 8));
            buf.write((byte)i);

            map.add(i, buf);

            buf.reset();
        }
    }


    @Test
    public void retainsMappingsFromOrdinalsToByteSequences() {
        ByteDataBuffer buf = new ByteDataBuffer();

        for(int i=0;i<1000;i++) {
            buf.reset();

            map.get(i, buf);

            Assert.assertEquals((byte)(i >> 8), buf.get(0));
            Assert.assertEquals((byte)i, buf.get(1));
        }
    }

    @Test
    public void compactsRemovedOrdinals() {
        for(int i=1;i<1000;i+=2) {
            map.remove(i);
        }

        Assert.assertEquals(2000, map.lengthOfByteData());

        map.compactByteData();

        Assert.assertEquals(1000, map.lengthOfByteData());
    }

    @Test
    public void remainingElementsAreAccessibleAfterCompaction() {
        ByteDataBuffer buf = new ByteDataBuffer();

        for(int i=1;i<1000;i+=2) {
            map.remove(i);
        }

        map.compactByteData();

        for(int i=0;i<1000;i+=2) {
            buf.reset();

            map.get(i, buf);

            Assert.assertEquals((byte)(i >> 8), buf.get(0));
            Assert.assertEquals((byte)i, buf.get(1));
        }
    }

}
