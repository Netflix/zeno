package com.netflix.zeno.fastblob;

import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.serializer.common.IntegerSerializer;
import com.netflix.zeno.serializer.common.StringSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FastBlobEngineTest {

    SerializerFactory factory = new SerializerFactory() {

        @Override
        public NFTypeSerializer<?>[] createSerializers() {
            return new NFTypeSerializer<?>[] { new IntegerSerializer(), new StringSerializer() };
        }
    };

    FastBlobStateEngine srcEngine1;
    FastBlobStateEngine srcEngine2;
    FastBlobStateEngine destEngine;

    @Before
    public void setUp() {
        srcEngine1 = new FastBlobStateEngine(factory, 2);
        srcEngine2 = new FastBlobStateEngine(factory, 2);
        destEngine = new FastBlobStateEngine(factory, 2);
    }

    @Test
    public void copiesDataFromOneStateEngineToAnother() throws Exception {
        /// initialize data in "from" state
        addData(srcEngine1, 1, true, true);
        addData(srcEngine1, 2, true, false);
        addData(srcEngine1, 3, false, true);

        copyEngine(srcEngine1, destEngine);

        /// assert data was copied
        assertData(destEngine, 1, true, true);
        assertData(destEngine, 2, true, false);
        assertData(destEngine, 3, false, true);
    }

    @Test
    public void copiesDataFromOneStateEngineToAnotherWithIgnoreList() throws Exception {
        /// initialize data in "from" state
        addData(srcEngine1, 1, true, true);
        addStringData(srcEngine1, "Two", true, false);
        addData(srcEngine1, 3, false, true);

        srcEngine1.copySerializationStatesTo(destEngine, Arrays.asList("Strings"));

        /// assert data was copied
        assertData(destEngine, 1, true, true);
        assertNoStringData(destEngine, "Two", true, true);
        assertData(destEngine, 3, false, true);
    }

    @Test
    public void copiesPartialDataFromOneStateEngineToAnotherInMultiplePhases() throws Exception {
        /// initialize data in "from" state
        addData(srcEngine1, 1, true, true);
        addStringData(srcEngine1, "Two", true, false);
        addData(srcEngine1, 3, false, true);

        OrdinalMapping mapping = srcEngine1.copySerializationStatesTo(destEngine, Arrays.asList("Strings"));
        srcEngine1.copySpecificSerializationStatesTo(destEngine, Arrays.asList("Strings"), mapping);

        /// assert data was copied
        assertData(destEngine, 1, true, true);
        assertStringData(destEngine, "Two", true, false);
        assertData(destEngine, 3, false, true);
    }

    @Test
    public void copiesDataFromOneStateEngineToAnotherWithIgnoreListContainingUnknownSerializer() throws Exception {
        /// initialize data in "from" state
        addData(srcEngine1, 1, true, true);
        addStringData(srcEngine1, "Two", true, false);
        addData(srcEngine1, 3, false, true);

        srcEngine1.copySerializationStatesTo(destEngine, Arrays.asList("Strings", "Foo"));

        /// assert data was copied
        assertData(destEngine, 1, true, true);
        assertNoStringData(destEngine, "Two", true, true);
        assertData(destEngine, 3, false, true);
    }

    @Test
    public void copiesDataFromMultipleStateEnginesToAnother() throws Exception {
        /// initialize data in "from" state
        addData(srcEngine1, 1, false, true);
        // Repetitive addition with different image flags
        addData(srcEngine1, 1, true, true);
        addData(srcEngine1, 2, true, false);
        addData(srcEngine1, 3, false, true);

        copyEngine(srcEngine1, destEngine);

        addData(srcEngine2, 1, true, true);
        addData(srcEngine2, 4, true, false);
        addData(srcEngine2, 5, false, true);

        copyEngine(srcEngine2, destEngine);

        /// assert data was copied
        assertData(destEngine, 1, true, true);
        assertData(destEngine, 2, true, false);
        assertData(destEngine, 3, false, true);
        assertData(destEngine, 4, true, false);
        assertData(destEngine, 5, false, true);

    }

    @Test
    public void serializeAndDeserialize() throws Exception{
        addData(srcEngine1, 1, true, true);
        addData(srcEngine1, 2, true, false);
        addData(srcEngine1, 3, false, true);
        addData(srcEngine1, 1, true, true);
        addData(srcEngine1, 2, true, false);
        addData(srcEngine1, 4, false, true);

        final File f = File.createTempFile("pre", "suf");
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
        DataInputStream dis = new DataInputStream(new FileInputStream(f));
        try {
            srcEngine1.setLatestVersion("foo");
            srcEngine1.serializeTo(dos);
            destEngine.deserializeFrom(dis);
        }finally {
            dos.close();
            dis.close();
            f.delete();
        }

        /// assert data was deserialized
        assertData(destEngine, 1, true, true);
        assertData(destEngine, 2, true, false);
        assertData(destEngine, 3, false, true);
        assertData(destEngine, 4, false, true);
    }

    private void copyEngine(FastBlobStateEngine srcStateEngine, FastBlobStateEngine destStateEngine) {
        srcStateEngine.copySerializationStatesTo(destStateEngine, Collections.<String> emptyList());
    }

    private void addData(FastBlobStateEngine stateEngine, Integer data, boolean... images) {
        stateEngine.add("Integer", data, FastBlobImageUtils.toLong(images));
    }

    private void addStringData(FastBlobStateEngine stateEngine, String data, boolean... images) {
        stateEngine.add("Strings", data, FastBlobImageUtils.toLong(images));
    }

    private void assertData(FastBlobStateEngine stateEngine, Integer data, boolean... images) throws Exception {
        stateEngine.prepareForWrite();

        for(int i=0;i<images.length;i++) {
            if(images[i]) {
                FastBlobStateEngine testStateEngine = new FastBlobStateEngine(factory);
                fillDeserializationWithImage(stateEngine, testStateEngine, i);

                Assert.assertTrue(containsData(testStateEngine, data, "Integer"));
            }
        }
    }

    private void assertNoStringData(FastBlobStateEngine stateEngine, String data, boolean... images) throws Exception {
        stateEngine.prepareForWrite();

        for(int i=0;i<images.length;i++) {
            if(images[i]) {
                FastBlobStateEngine testStateEngine = new FastBlobStateEngine(factory);
                fillDeserializationWithImage(stateEngine, testStateEngine, i);

                Assert.assertFalse(containsData(testStateEngine, data, "Strings"));
            }
        }
    }

    private void assertStringData(FastBlobStateEngine stateEngine, String data, boolean... images) throws Exception {
        stateEngine.prepareForWrite();

        for(int i=0;i<images.length;i++) {
            if(images[i]) {
                FastBlobStateEngine testStateEngine = new FastBlobStateEngine(factory);
                fillDeserializationWithImage(stateEngine, testStateEngine, i);

                Assert.assertTrue(containsData(testStateEngine, data, "Strings"));
            }
        }
    }

    private <T> boolean containsData(FastBlobStateEngine stateEngine, T value, String serializerName) {
        FastBlobTypeDeserializationState<T> typeState = stateEngine.getTypeDeserializationState(serializerName);
        for(T i : typeState) {
            if(i.equals(value))
                return true;
        }
        return false;
    }

    private void fillDeserializationWithImage(FastBlobStateEngine serverStateEngine, FastBlobStateEngine clientStateEngine,
            int imageIndex) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FastBlobWriter writer = new FastBlobWriter(serverStateEngine, imageIndex);
        writer.writeSnapshot(baos);

        FastBlobReader reader = new FastBlobReader(clientStateEngine);
        reader.readSnapshot(new ByteArrayInputStream(baos.toByteArray()));
    }
}
