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
package com.netflix.zeno.fastblob.state;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.testpojos.TypeF;
import com.netflix.zeno.testpojos.TypeFSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TypeDeserializationStateListenerTest {

    FastBlobStateEngine stateEngine;

    byte snapshot1[];

    byte delta[];
    byte snapshot2[];

    @Before
    public void createStates() throws Exception {
        stateEngine = new FastBlobStateEngine(new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeFSerializer() };
            }
        });

        /// first state has 1 - 10
        addFs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        stateEngine.prepareForWrite();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        FastBlobWriter writer = new FastBlobWriter(stateEngine);
        writer.writeSnapshot(baos);
        snapshot1 = baos.toByteArray();
        baos.reset();

        stateEngine.prepareForNextCycle();

        /// second state removes 3, 7, 10 and adds 11, 12
        addFs(1, 2, 4, 5, 6, 8, 9, 11, 12);

        stateEngine.prepareForWrite();

        writer.writeDelta(baos);
        delta = baos.toByteArray();
        baos.reset();

        writer.writeSnapshot(baos);
        snapshot2 = baos.toByteArray();
   }

    private void addFs(int... values) {
        for(int value : values)
            stateEngine.add("TypeF", new TypeF(value));
    }


    @Test
    public void testListenerSnapshot() throws IOException {
        TestTypeDeserializationStateListener listener = new TestTypeDeserializationStateListener();
        stateEngine.setTypeDeserializationStateListener("TypeF", listener);
        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(snapshot1));

        Assert.assertTrue(listener.getRemovedValues().isEmpty());
        Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), listener.getAddedValues());
    }

    @Test
    public void testListenerDelta() throws IOException {
        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(snapshot1));

        TestTypeDeserializationStateListener listener = new TestTypeDeserializationStateListener();
        stateEngine.setTypeDeserializationStateListener("TypeF", listener);
        reader.readDelta(new ByteArrayInputStream(delta));

        Assert.assertEquals(Arrays.asList(3, 7, 10), listener.getRemovedValues());
        Assert.assertEquals(Arrays.asList(11, 12), listener.getAddedValues());
    }

    @Test
    public void testListenerDoubleSnapshot() throws IOException {
        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(snapshot1));

        TestTypeDeserializationStateListener listener = new TestTypeDeserializationStateListener();
        stateEngine.setTypeDeserializationStateListener("TypeF", listener);
        reader.readSnapshot(new ByteArrayInputStream(snapshot2));

        Assert.assertEquals(Arrays.asList(3, 7, 10), listener.getRemovedValues());
        Assert.assertEquals(Arrays.asList(11, 12), listener.getAddedValues());

    }

    private static class TestTypeDeserializationStateListener extends TypeDeserializationStateListener<TypeF> {
        List<Integer> removedValues = new ArrayList<Integer>();
        List<Integer> addedValues = new ArrayList<Integer>();

        @Override
        public void removedObject(TypeF obj, int ordinal) {
            removedValues.add(obj.getValue());
        }

        @Override
        public void addedObject(TypeF obj, int ordinal) {
            addedValues.add(obj.getValue());
        }

        public List<Integer> getRemovedValues() {
            return removedValues;
        }

        public List<Integer> getAddedValues() {
            return addedValues;
        }

    }

}
