package com.netflix.zeno.flatblob;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.testpojos.TypeA;
import com.netflix.zeno.testpojos.TypeD;
import com.netflix.zeno.testpojos.TypeDSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FlatBlobTest {

    private SerializerFactory typeDSerializerFactory;
    private FastBlobStateEngine stateEngine;
    private FlatBlobSerializationFramework flatBlobFramework;

    private TypeD d1;
    private TypeD d2;
    private TypeD d3;

    @Before
    public void setUp() throws Exception {
        typeDSerializerFactory = new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeDSerializer() };
            }
        };

        stateEngine = new FastBlobStateEngine(typeDSerializerFactory);

        stateEngine.add("TypeD", typeD(1, 2));
        stateEngine.add("TypeD", typeD(2, 2));
        stateEngine.add("TypeD", typeD(3, 2));

        roundTripStateEngine(stateEngine);

        d1 = (TypeD) stateEngine.getTypeDeserializationState("TypeD").get(0);
        d2 = (TypeD) stateEngine.getTypeDeserializationState("TypeD").get(1);
        d3 = (TypeD) stateEngine.getTypeDeserializationState("TypeD").get(2);

        flatBlobFramework = new FlatBlobSerializationFramework(typeDSerializerFactory, stateEngine);
    }


    @Test
    public void cachingElementsResultsInDeduplication() {
        ByteDataBuffer d1Buf = new ByteDataBuffer();
        ByteDataBuffer d2Buf = new ByteDataBuffer();

        flatBlobFramework.serialize("TypeD", d1, d1Buf);
        flatBlobFramework.serialize("TypeD", d2, d2Buf);

        TypeD deserializedD1 = flatBlobFramework.deserialize("TypeD", d1Buf.getUnderlyingArray(), true);
        TypeD deserializedD2 = flatBlobFramework.deserialize("TypeD", d2Buf.getUnderlyingArray(), true);

        Assert.assertSame(deserializedD1.getTypeA(), deserializedD2.getTypeA());
    }

    @Test
    public void uncachedElementsAreNotDeduplicated() {
        ByteDataBuffer d1Buf = new ByteDataBuffer();
        ByteDataBuffer d2Buf = new ByteDataBuffer();

        flatBlobFramework.serialize("TypeD", d1, d1Buf);
        flatBlobFramework.serialize("TypeD", d2, d2Buf);

        TypeD deserializedD1 = flatBlobFramework.deserialize("TypeD", d1Buf.getUnderlyingArray(), false);
        TypeD deserializedD2 = flatBlobFramework.deserialize("TypeD", d2Buf.getUnderlyingArray(), false);

        Assert.assertNotSame(deserializedD1.getTypeA(), deserializedD2.getTypeA());
    }

    @Test
    public void evictionOfCachedElementsWillResultInNewObjects() {
        FlatBlobEvictor evictor = new FlatBlobEvictor(typeDSerializerFactory, flatBlobFramework);

        ByteDataBuffer d1Buf = new ByteDataBuffer();
        ByteDataBuffer d2Buf = new ByteDataBuffer();

        flatBlobFramework.serialize("TypeD", d1, d1Buf);
        flatBlobFramework.serialize("TypeD", d2, d2Buf);

        TypeD deserializedD1 = flatBlobFramework.deserialize("TypeD", d1Buf.getUnderlyingArray(), true);

        evictor.evict("TypeD", deserializedD1);

        TypeD deserializedD2 = flatBlobFramework.deserialize("TypeD", d2Buf.getUnderlyingArray(), true);

        Assert.assertNotSame(deserializedD1.getTypeA(), deserializedD2.getTypeA());
    }

    @Test
    public void evictorCountsReferencesAndDoesNotEvictObjectsTooEarly() {
        FlatBlobEvictor evictor = new FlatBlobEvictor(typeDSerializerFactory, flatBlobFramework);

        ByteDataBuffer d1Buf = new ByteDataBuffer();
        ByteDataBuffer d2Buf = new ByteDataBuffer();
        ByteDataBuffer d3Buf = new ByteDataBuffer();

        flatBlobFramework.serialize("TypeD", d1, d1Buf);
        flatBlobFramework.serialize("TypeD", d2, d2Buf);
        flatBlobFramework.serialize("TypeD", d3, d3Buf);

        TypeD deserializedD1 = flatBlobFramework.deserialize("TypeD", d1Buf.getUnderlyingArray(), true);
        TypeD deserializedD2 = flatBlobFramework.deserialize("TypeD", d1Buf.getUnderlyingArray(), true);

        evictor.evict("TypeD", deserializedD1);

        TypeD deserializedD3 = flatBlobFramework.deserialize("TypeD", d2Buf.getUnderlyingArray(), true);

        Assert.assertSame(deserializedD2.getTypeA(), deserializedD3.getTypeA());
    }

    private TypeD typeD(int dVal, int aVal) {
        return new TypeD(dVal, new TypeA(aVal, aVal));
    }

    private void roundTripStateEngine(FastBlobStateEngine stateEngine) throws Exception, IOException {
        stateEngine.prepareForWrite();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new FastBlobWriter(stateEngine).writeSnapshot(baos);
        new FastBlobReader(stateEngine).readSnapshot(new ByteArrayInputStream(baos.toByteArray()));
    }

}
