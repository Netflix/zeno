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
package com.netflix.zeno.fastblob.restorescenarios;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.io.FastBlobReader;
import com.netflix.zeno.fastblob.io.FastBlobWriter;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

public class RestoreServerAfterSchemaChangeTest {


    @Test
    public void addingANewSerializerAndChangingTheFieldsInExistingSerializer() throws IOException {
        // create a server's state engine with an initial set of serializers
        FastBlobStateEngine stateEngine1 = new FastBlobStateEngine(state1Factory());

        /// add some data to that server
        stateEngine1.add("TypeA", new TypeAState1(1));
        stateEngine1.add("TypeA", new TypeAState1(2));
        stateEngine1.add("TypeA", new TypeAState1(3));

        stateEngine1.add("TypeB", new TypeB(1));
        stateEngine1.add("TypeB", new TypeB(2));
        stateEngine1.add("TypeB", new TypeB(3));

        /// set the latest version
        stateEngine1.setLatestVersion("test1");

        /// serialize that data
        stateEngine1.prepareForWrite();

        ByteArrayOutputStream serializedSnapshotState1 = new ByteArrayOutputStream();

        new FastBlobWriter(stateEngine1, 0).writeSnapshot(new DataOutputStream(serializedSnapshotState1));

        /// serialize the state engine
        ByteArrayOutputStream serializedStateEngine1 = new ByteArrayOutputStream();

        stateEngine1.serializeTo(serializedStateEngine1);

        /// server is restarted with updated serializers
        FastBlobStateEngine stateEngine2 = new FastBlobStateEngine(state2Factory());

        /// deserialize the state engine from the previous server (with the old serializers)
        stateEngine2.deserializeFrom(new ByteArrayInputStream(serializedStateEngine1.toByteArray()));
        stateEngine2.prepareForNextCycle();

        /// add new data to the state engine, with some overlap
        TypeC c1 = new TypeC(1);
        TypeC c2 = new TypeC(2);
        TypeC c3 = new TypeC(3);

        stateEngine2.add("TypeA", new TypeAState2(1, c1));
        stateEngine2.add("TypeA", new TypeAState2(2, c2));
        stateEngine2.add("TypeA", new TypeAState2(4, c3));

        stateEngine2.add("TypeB", new TypeB(1));
        stateEngine2.add("TypeB", new TypeB(9));
        stateEngine2.add("TypeB", new TypeB(3));

        stateEngine2.setLatestVersion("test");

        /// serialize a delta between the previous state and this state
        stateEngine2.prepareForWrite();

        ByteArrayOutputStream serializedDeltaState2 = new ByteArrayOutputStream();

        new FastBlobWriter(stateEngine2, 0).writeDelta(new DataOutputStream(serializedDeltaState2));


        /// now we need to deserialize.  Deserialize first the snapshot produced by server 1, then the delta produced by server 2 (with different serializers)
        new FastBlobReader(stateEngine1).readSnapshot(new ByteArrayInputStream(serializedSnapshotState1.toByteArray()));
        new FastBlobReader(stateEngine1).readDelta(new ByteArrayInputStream(serializedDeltaState2.toByteArray()));

        /// get the type As
        FastBlobTypeDeserializationState<TypeAState1> typeAs = stateEngine1.getTypeDeserializationState("TypeA");

        /// here we use some implicit knowledge about how the state engine works to verify the functionality...
        /// A's serializer changed, so the ordinals for the new objects should not overlap with any of the original
        /// ordinals (0, 1, 2).
        Assert.assertEquals(1, typeAs.get(3).getA1());
        Assert.assertEquals(2, typeAs.get(4).getA1());
        Assert.assertEquals(4, typeAs.get(5).getA1());

        FastBlobTypeDeserializationState<TypeB> typeBs = stateEngine1.getTypeDeserializationState("TypeB");

        /// B's serializer did not change.  The ordinals for objects which exist across states will be reused (0, 2)
        /// one new ordinal will be added for the changed object (3)
        Assert.assertEquals(1, typeBs.get(0).getB1());
        Assert.assertEquals(3, typeBs.get(2).getB1());
        Assert.assertEquals(9, typeBs.get(3).getB1());

        FastBlobStateEngine removedTypeBStateEngine = new FastBlobStateEngine(removedTypeBFactory());

        new FastBlobReader(removedTypeBStateEngine).readSnapshot(new ByteArrayInputStream(serializedSnapshotState1.toByteArray()));
        new FastBlobReader(removedTypeBStateEngine).readDelta(new ByteArrayInputStream(serializedDeltaState2.toByteArray()));


        /// get the type As
        FastBlobTypeDeserializationState<TypeAState2> typeA2s = removedTypeBStateEngine.getTypeDeserializationState("TypeA");

        /// here we use some implicit knowledge about how the state engine works to verify the functionality...
        /// A's serializer changed, so the ordinals for the new objects should not overlap with any of the original
        /// ordinals (0, 1, 2).
        Assert.assertEquals(1, typeA2s.get(3).getC().getC1());
        Assert.assertEquals(2, typeA2s.get(4).getC().getC1());
        Assert.assertEquals(3, typeA2s.get(5).getC().getC1());

    }


    private SerializerFactory removedTypeBFactory() {
        return new SerializerFactory() {
            @Override
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] {
                        new AState2Serializer()
                };
            }
        };
    }


    private SerializerFactory state2Factory() {
        return new SerializerFactory() {
            @Override
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] {
                        new AState2Serializer(),
                        new BSerializer()
                };
            }
        };
    }

    private SerializerFactory state1Factory() {
        return new SerializerFactory() {
            @Override
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] {
                        new AState1Serializer(),
                        new BSerializer()
                };
            }
        };
    }


}
