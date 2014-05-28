package com.netflix.zeno.fastblob.state;

import com.netflix.zeno.fastblob.FastBlobImageUtils;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.serializer.common.IntegerSerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FastBlobTypeSerializationStateTest {

    FastBlobTypeSerializationState<Integer> srcState;
    FastBlobTypeSerializationState<Integer> destState;

    @Before
    public void setUp() {
        srcState = new FastBlobTypeSerializationState<Integer>(new IntegerSerializer(), 2);
        destState = new FastBlobTypeSerializationState<Integer>(new IntegerSerializer(), 2);
    }

    @Test
    public void serializeAndDeserialize() throws Exception{
        /// initialize data in "from" state
        addData(srcState, new byte[] { 1, 2 }, true, true);
        addData(srcState, new byte[] { 3, 4, 5 }, true, false);
        addData(srcState, new byte[] { 6, 7, 8, 9 }, false, true);

        final File f = File.createTempFile("pre", "suf");
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
        srcState.serializeTo(dos);
        dos.close();

        DataInputStream dis = new DataInputStream(new FileInputStream(f));
        destState.deserializeFrom(dis, 2);
        dis.close();

        /// assert data was copied
        assertData(destState, new byte[] { 1, 2 }, true, true);
        assertData(destState, new byte[] { 3, 4, 5 }, true, false);
        assertData(destState, new byte[] { 6, 7, 8, 9 }, false, true);
        f.delete();
    }

    private void addData(FastBlobTypeSerializationState<Integer> srcState, byte data[], boolean... images) {
        ByteDataBuffer buf = createBuffer(data);
        srcState.addData(buf, FastBlobImageUtils.toLong(images));
    }

    private void assertData(FastBlobTypeSerializationState<Integer> destState, byte data[], boolean... images) {
        /// get the ordinal for the data, but don't add it to any images
        int ordinal = destState.addData(createBuffer(data), FastBlobImageUtils.toLong(false, false));
        /// see which images this data was added to
        Assert.assertEquals(images[0], destState.getImageMembershipBitSet(0).get(ordinal));
        Assert.assertEquals(images[1], destState.getImageMembershipBitSet(1).get(ordinal));
    }

    private ByteDataBuffer createBuffer(byte[] data) {
        ByteDataBuffer buf = new ByteDataBuffer();
        for(int i=0;i<data.length;i++)
            buf.write(data[i]);
        return buf;
    }

}
