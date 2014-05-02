package com.netflix.zeno.fastblob.state.compressed;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;

import org.junit.Assert;
import org.junit.Test;

public class ByteSequenceRetainerTest {

    @Test
    public void retainsByteSequences() {
        ByteSequenceRetainer retainer = new ByteSequenceRetainer();

        ByteDataBuffer buf = new ByteDataBuffer();
        buf.write((byte)1);
        buf.write((byte)2);
        buf.write((byte)3);

        retainer.addByteSequence(10, buf.getUnderlyingArray(), 0, 3);

        ByteDataBuffer retrieved = new ByteDataBuffer();

        int retrievedLength = retainer.retrieveSequence(10, retrieved);

        Assert.assertEquals(3, retrievedLength);
        Assert.assertEquals(1, retrieved.get(0));
        Assert.assertEquals(2, retrieved.get(1));
        Assert.assertEquals(3, retrieved.get(2));
    }

}
