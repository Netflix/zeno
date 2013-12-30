package com.netflix.zeno.diff.history;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.testpojos.TypeASerializer;

public class DiffHistoryAbstractTest {

    protected SerializerFactory serializerFactory() {
        return new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeASerializer() };
            }
        };
    }
    
    protected void roundTripObjects(FastBlobStateEngine stateEngine) throws IOException {
        stateEngine.prepareForWrite();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        FastBlobWriter writer = new FastBlobWriter(stateEngine);
        writer.writeSnapshot(new DataOutputStream(baos));

        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(baos.toByteArray()));

        stateEngine.prepareForNextCycle();
     }
    

}
