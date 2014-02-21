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

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class CompatibilitySerializerTest {
    SerializerFactory serializerFactory = new SerializerFactory() {
        @Override
        public NFTypeSerializer<?>[] createSerializers() {
            return new NFTypeSerializer<?>[]{ new TestSerializer1(), new TestSerializer2() };
        }
    };
    FastBlobStateEngine framework = new FastBlobStateEngine(serializerFactory);

    NFTypeSerializer<String[]> testSerializer1 = framework.getSerializer(TestSerializer1.NAME);
    NFTypeSerializer<String[]> testSerializer2 = framework.getSerializer(TestSerializer2.NAME);

    String text1 = "String1";
    String text2 = "String2";

    @Test
    public void testRemove() throws Exception {

        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(testSerializer2.getFastBlobSchema());
        testSerializer2.serialize(new String[]{text1, text2}, rec);

        FastBlobDeserializationRecord result = serializeDeserialize(testSerializer2, rec);

        String[] deserialized = testSerializer1.deserialize(result);

        Assert.assertEquals(text1, deserialized[0]);
    }

    @Test
    public void testAdd() throws Exception {

        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(testSerializer1.getFastBlobSchema());
        testSerializer1.serialize(new String[]{text1}, rec);
        FastBlobDeserializationRecord result = serializeDeserialize(testSerializer1, rec);

        String[] deserialized = testSerializer2.deserialize(result);

        Assert.assertEquals(text1, deserialized[0]);
        Assert.assertEquals(null, deserialized[1]);
    }

    private FastBlobDeserializationRecord serializeDeserialize(NFTypeSerializer<String[]> testSerializer, FastBlobSerializationRecord rec) throws IOException {

        ByteDataBuffer buf = new ByteDataBuffer();

        rec.writeDataTo(buf);

        FastBlobDeserializationRecord deserializeRec = new FastBlobDeserializationRecord(testSerializer.getFastBlobSchema(), buf.getUnderlyingArray());

        deserializeRec.position(0);

        return deserializeRec;
    }

    public class TestSerializer1 extends NFTypeSerializer<String[]> {

        public static final String NAME = "TEST1";

        public TestSerializer1() {
            super(NAME);
        }

        @Override
        public void doSerialize(String[] objs, NFSerializationRecord rec) {
            serializePrimitive(rec, "string1", (String)objs[0]);
        }

        @Override
        protected String[] doDeserialize(NFDeserializationRecord rec) {
            return new String[]{
                    deserializePrimitiveString(rec, "string1")
            };
        }

        @Override
        protected FastBlobSchema createSchema() {
            return schema(
                    field("string1", FieldType.STRING)
            );
        }

        @Override
        public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
            return Collections.emptySet();
        }

    }

    public class TestSerializer2 extends NFTypeSerializer<String[]> {

        public static final String NAME = "TEST2";

        public TestSerializer2() {
            super(NAME);
        }

        @Override
        public void doSerialize(String[] objs, NFSerializationRecord rec) {
            serializePrimitive(rec, "string1", (String)objs[0]);
            serializePrimitive(rec, "string2", (String)objs[1]);
        }

        @Override
        protected String[] doDeserialize(NFDeserializationRecord rec) {
            return new String[]{
                    deserializePrimitiveString(rec, "string1"),
                    deserializePrimitiveString(rec, "string2")
            };
        }

        @Override
        protected FastBlobSchema createSchema() {
            return schema(
                    field("string1", FieldType.STRING),
                    field("string2", FieldType.STRING)
            );
        }

        @Override
        public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
            return Collections.emptySet();
        }

    }


}
