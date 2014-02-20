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
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TypeDeserializationStateListenerTest {

    FastBlobStateEngine stateEngine;

    byte snapshot1[];

    byte delta[];
    byte snapshot2[];

    byte brokenDeltaChainSnapshot2[];

    @Before
    public void createStates() throws Exception {
        stateEngine = newStateEngine();

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
        baos.reset();

        /// we also create a broken delta chain to cause ordinal reassignments.
        stateEngine = newStateEngine();

        addFs(1, 2, 4, 5, 6, 8, 9, 11, 12);

        stateEngine.prepareForWrite();

        writer = new FastBlobWriter(stateEngine);
        writer.writeSnapshot(baos);
        brokenDeltaChainSnapshot2 = baos.toByteArray();
   }

    private FastBlobStateEngine newStateEngine() {
        return new FastBlobStateEngine(new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeFSerializer() };
            }
        });
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

        Assert.assertEquals(0, listener.getRemovedValuesSize());
        Assert.assertEquals(10, listener.getAddedValuesSize());
        Assert.assertEquals(3, listener.getAddedValueOrdinal(4));
    }

    @Test
    public void testListenerDelta() throws IOException {
        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(snapshot1));

        TestTypeDeserializationStateListener listener = new TestTypeDeserializationStateListener();
        stateEngine.setTypeDeserializationStateListener("TypeF", listener);
        reader.readDelta(new ByteArrayInputStream(delta));

        Assert.assertEquals(3, listener.getRemovedValuesSize());
        Assert.assertEquals(2, listener.getRemovedValueOrdinal(3));
        Assert.assertEquals(2, listener.getAddedValuesSize());
        Assert.assertEquals(10, listener.getAddedValueOrdinal(11));
    }

    @Test
    public void testListenerDoubleSnapshot() throws IOException {
        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(snapshot1));

        TestTypeDeserializationStateListener listener = new TestTypeDeserializationStateListener();
        stateEngine.setTypeDeserializationStateListener("TypeF", listener);
        reader.readSnapshot(new ByteArrayInputStream(snapshot2));

        Assert.assertEquals(3, listener.getRemovedValuesSize());
        Assert.assertEquals(2, listener.getRemovedValueOrdinal(3));
        Assert.assertEquals(2, listener.getAddedValuesSize());
        Assert.assertEquals(10, listener.getAddedValueOrdinal(11));
        Assert.assertEquals(7, listener.getReassignedValuesSize());
        Assert.assertEquals(new OrdinalReassignment(3, 3), listener.getOrdinalReassignment(4));
    }

    @Test
    public void testListenerDoubleSnapshotDiscontinuousState() throws IOException {
        FastBlobReader reader = new FastBlobReader(stateEngine);
        reader.readSnapshot(new ByteArrayInputStream(snapshot1));

        TestTypeDeserializationStateListener listener = new TestTypeDeserializationStateListener();
        stateEngine.setTypeDeserializationStateListener("TypeF", listener);
        reader.readSnapshot(new ByteArrayInputStream(brokenDeltaChainSnapshot2));

        Assert.assertEquals(3, listener.getRemovedValuesSize());
        Assert.assertEquals(2, listener.getRemovedValueOrdinal(3));
        Assert.assertEquals(2, listener.getAddedValuesSize());
        Assert.assertEquals(7, listener.getAddedValueOrdinal(11));
        Assert.assertEquals(7, listener.getReassignedValuesSize());
        Assert.assertEquals(new OrdinalReassignment(3, 2), listener.getOrdinalReassignment(4));
        Assert.assertEquals(new OrdinalReassignment(8, 6), listener.getOrdinalReassignment(9));
    }


    private static class TestTypeDeserializationStateListener extends TypeDeserializationStateListener<TypeF> {
        Map<Integer, Integer> removedValuesAndOrdinals = new HashMap<Integer, Integer>();
        Map<Integer, Integer> addedValuesAndOrdinals = new HashMap<Integer, Integer>();
        Map<Integer, OrdinalReassignment> reassignedValues = new HashMap<Integer, OrdinalReassignment>();

        @Override
        public void removedObject(TypeF obj, int ordinal) {
            removedValuesAndOrdinals.put(obj.getValue(), ordinal);
        }

        @Override
        public void addedObject(TypeF obj, int ordinal) {
            addedValuesAndOrdinals.put(obj.getValue(), ordinal);
        }

        @Override
        public void reassignedObject(TypeF obj, int oldOrdinal, int newOrdinal) {
            OrdinalReassignment reassignment = new OrdinalReassignment(oldOrdinal, newOrdinal);
            reassignment.oldOrdinal = oldOrdinal;
            reassignment.newOrdinal = newOrdinal;
            reassignedValues.put(obj.getValue(), reassignment);
        }

        public int getRemovedValuesSize() {
            return removedValuesAndOrdinals.size();
        }

        public int getRemovedValueOrdinal(Integer value) {
            return removedValuesAndOrdinals.get(value);
        }

        public int getAddedValuesSize() {
            return addedValuesAndOrdinals.size();
        }

        public int getAddedValueOrdinal(Integer value) {
            return addedValuesAndOrdinals.get(value);
        }

        public int getReassignedValuesSize() {
            return reassignedValues.size();
        }

        public OrdinalReassignment getOrdinalReassignment(Integer value) {
            return reassignedValues.get(value);
        }


    }

    static class OrdinalReassignment {
        private int oldOrdinal;
        private int newOrdinal;

        public OrdinalReassignment(int oldOrdinal, int newOrdinal) {
            this.oldOrdinal = oldOrdinal;
            this.newOrdinal = newOrdinal;
        }

        public boolean equals(Object other) {
            if(other instanceof OrdinalReassignment) {
                OrdinalReassignment otherOR = (OrdinalReassignment)other;
                return oldOrdinal == otherOR.oldOrdinal && newOrdinal == otherOR.newOrdinal;
            }
            return false;
        }
    }
}
