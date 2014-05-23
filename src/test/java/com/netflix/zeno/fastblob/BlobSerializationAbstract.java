/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.zeno.fastblob;

import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class BlobSerializationAbstract {

    protected FastBlobStateEngine serializationState;

    protected ByteArrayOutputStream baos;


    public void setUp() throws Exception {
        this.baos = new ByteArrayOutputStream();
    }

    protected void serializeAndDeserializeDelta() throws IOException, Exception {
        serializationState.prepareForWrite();
        new FastBlobWriter(serializationState, 0).writeDelta(new DataOutputStream(baos));
        serializationState.prepareForNextCycle();
        new FastBlobReader(serializationState).readDelta(new ByteArrayInputStream(baos.toByteArray()));
        baos.reset();
    }

    protected void serializeAndDeserializeSnapshot() throws Exception {
        final byte[] data = serializeSnapshot();
        deserializeSnapshot(data);
    }

    protected void deserializeSnapshot(final byte[] data) throws IOException {
        final InputStream is = new ByteArrayInputStream(data);
        new FastBlobReader(serializationState).readSnapshot(is);
    }

    protected byte[] serializeSnapshot() throws IOException {
        serializationState.prepareForWrite();
        new FastBlobWriter(serializationState, 0).writeSnapshot(new DataOutputStream(baos));
        serializationState.prepareForNextCycle();
        baos.close();
        final byte data[] = baos.toByteArray();
        baos.reset();
        return data;
    }

    protected void cache(final String cacheName, Object obj) {
        serializationState.add(cacheName, obj, FastBlobImageUtils.ONE_TRUE);
    }

    protected <T> List<T> getAll(final String cacheName) {
        List<T> list = new ArrayList<T>();

        FastBlobTypeDeserializationState<T> state = serializationState.getTypeDeserializationState(cacheName);

        for(T t : state)
            list.add(t);

        return list;
    }

}
