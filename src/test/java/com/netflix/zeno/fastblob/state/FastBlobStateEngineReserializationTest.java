package com.netflix.zeno.fastblob.state;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.testpojos.TypeA;
import com.netflix.zeno.testpojos.TypeASerializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class FastBlobStateEngineReserializationTest {

    @Test
    public void canReserializeDeserializedData() throws Exception {
        FastBlobStateEngine stateEngine = typeAStateEngine();

        stateEngine.add("TypeA", new TypeA(1, 2));
        stateEngine.add("TypeA", new TypeA(3, 4));

        stateEngine.prepareForWrite();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        FastBlobWriter writer = new FastBlobWriter(stateEngine);
        writer.writeSnapshot(baos);

        FastBlobStateEngine deserializeEngine = typeAStateEngine();

        FastBlobReader reader = new FastBlobReader(deserializeEngine);

        reader.readSnapshot(new ByteArrayInputStream(baos.toByteArray()));

        deserializeEngine.fillSerializationStatesFromDeserializedData();

        deserializeEngine.prepareForWrite();

        ByteArrayOutputStream reserializedStream = new ByteArrayOutputStream();

        FastBlobWriter rewriter = new FastBlobWriter(deserializeEngine);

        rewriter.writeSnapshot(reserializedStream);

        Assert.assertArrayEquals(baos.toByteArray(), reserializedStream.toByteArray());

        System.out.println(Arrays.toString(baos.toByteArray()));
        System.out.println(Arrays.toString(reserializedStream.toByteArray()));

    }

    private FastBlobStateEngine typeAStateEngine() {
        return new FastBlobStateEngine(new SerializerFactory() {
            @Override
            public NFTypeSerializer<?>[] createSerializers() {
                // TODO Auto-generated method stub
                return new NFTypeSerializer<?>[] { new TypeASerializer() };
            }
        });
    }

}
