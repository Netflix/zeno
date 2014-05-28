package com.netflix.zeno.fastblob.io;

import static org.junit.Assert.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.Before;
import org.junit.Test;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.serializer.common.IntegerSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

public class FastBlobWriterTest {

    private final SerializerFactory factory = new SerializerFactory() {

        @Override
        public NFTypeSerializer<?>[] createSerializers() {
            return new NFTypeSerializer<?>[] { new IntegerSerializer(), new StringSerializer() };
        }
    };

    private FastBlobStateEngine srcEngine;
    private FastBlobStateEngine destEngine;
    private FastBlobWriter fastBlobWriter;
    private FastBlobReader fastBlobReader;

    @Before
    public void setUp() {
        srcEngine = new FastBlobStateEngine(factory, 2);
        destEngine = new FastBlobStateEngine(factory, 2);
    }

    @Test
    public void writeSnapshotFromStatesAndRead() throws Exception {
        addData(srcEngine, 1, true, true);
        addStringData(srcEngine, "Two", true, false);
        addData(srcEngine, 3, false, true);
        addStringData(srcEngine, "Four", false, false);
        addData(srcEngine, 5, false, true);

        srcEngine.fillDeserializationStatesFromSerializedData();
        srcEngine.prepareForWrite();
        fastBlobWriter = new FastBlobWriter(srcEngine);

        final File f = File.createTempFile("pre", "suf");
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
        fastBlobWriter.writeNonImageSpecificSnapshot(dos);
        dos.close();

        fastBlobReader = new FastBlobReader(destEngine);
        DataInputStream dis = new DataInputStream(new FileInputStream(f));
        fastBlobReader.readSnapshot(dis);
        dis.close();

        assertTrue(containsData(destEngine, 1, "Integer"));
        assertTrue(containsData(destEngine, "Two", "Strings"));
        assertTrue(containsData(destEngine, 3, "Integer"));
        assertTrue(containsData(destEngine, "Four", "Strings"));
        assertTrue(containsData(destEngine, 5, "Integer"));
    }


    private void addData(FastBlobStateEngine stateEngine, Integer data, boolean... images) {
        stateEngine.add("Integer", data, images);
    }

    private void addStringData(FastBlobStateEngine stateEngine, String data, boolean... images) {
        stateEngine.add("Strings", data, images);
    }

    private <T> boolean containsData(FastBlobStateEngine stateEngine, T value, String serializerName) {
        FastBlobTypeDeserializationState<T> typeState = stateEngine.getTypeDeserializationState(serializerName);
        for(T obj : typeState) {
            if(obj.equals(value))
                return true;
        }
        return false;
    }
}
