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

import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

public class NFTypeSerializerTest {
    SerializerFactory serializerFactory = new SerializerFactory() {
        @Override
        public NFTypeSerializer<?>[] createSerializers() {
            return new NFTypeSerializer<?>[]{ new TestSerializer(), new TestSerializerBytes() };
        }
    };
    FastBlobStateEngine framework = new FastBlobStateEngine(serializerFactory);


    @Test
    public void booleanUsesDefaultWhenActualValueIsNull() {
        TestSerializer testSerializer = new TestSerializer();
        testSerializer.setSerializationFramework(new FastBlobStateEngine(serializerFactory));
        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(testSerializer.getFastBlobSchema());
        testSerializer.serialize(null, rec);

        ByteDataBuffer buf = new ByteDataBuffer();

        rec.writeDataTo(buf);

        FastBlobDeserializationRecord deserializeRec = new FastBlobDeserializationRecord(testSerializer.getFastBlobSchema(), buf.getUnderlyingArray());
        deserializeRec.position(0);

        Boolean deserialized = testSerializer.deserialize(deserializeRec);

        Assert.assertEquals(Boolean.TRUE, deserialized);
    }

    public class TestSerializer extends NFTypeSerializer<Boolean> {

        private static final String NAME = "TEST_BOOLEAN";

        public TestSerializer() {
            super(NAME);
        }

        @Override
        public void doSerialize(Boolean value, NFSerializationRecord rec) {
            serializePrimitive(rec, "bool", value);
        }

        @Override
        protected Boolean doDeserialize(NFDeserializationRecord rec) {
            return deserializeBoolean(rec, "bool", true);
        }

        @Override
        protected FastBlobSchema createSchema() {
            return schema(
                    field("bool", FieldType.BOOLEAN)
            );
        }

        @Override
        public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
            return Collections.emptySet();
        }
    }

    byte[] bytes = new byte[]{1,55};
    byte[] bytes2 = new byte[]{2,56};

    @Test
    public void testBytes() throws Exception {
        NFTypeSerializer<Object[]> testSerializer = framework.getSerializer(TestSerializerBytes.NAME);
        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(testSerializer.getFastBlobSchema());
        testSerializer.serialize(new Object[]{bytes, bytes2}, rec);

        ByteDataBuffer buf = new ByteDataBuffer();

        rec.writeDataTo(buf);

        FastBlobDeserializationRecord deserializeRec = new FastBlobDeserializationRecord(testSerializer.getFastBlobSchema(), buf.getUnderlyingArray());
        deserializeRec.position(0);

        Object[] deserialized = testSerializer.deserialize(deserializeRec);

        Assert.assertArrayEquals(bytes, (byte[])deserialized[0]);
        Assert.assertArrayEquals(bytes2, (byte[])deserialized[1]);
    }

    public class TestSerializerBytes extends NFTypeSerializer<Object[]> {

        public static final String NAME = "TEST_BYTES";

        public TestSerializerBytes() {
            super(NAME);
        }

        @Override
        public void doSerialize(Object[] objs, NFSerializationRecord rec) {
            serializeBytes(rec, "bytes", (byte[])objs[0]);
            serializeBytes(rec, "bytes2", (byte[])objs[1]);
        }

        @Override
        protected Object[] doDeserialize(NFDeserializationRecord rec) {
            return new Object[]{
                    deserializeBytes(rec, "bytes"),
                    deserializeBytes(rec, "bytes2")
            };
        }

        @Override
        protected FastBlobSchema createSchema() {
            return schema(
                    field("bytes", FieldType.BYTES),
                    field("bytes2", FieldType.BYTES)
            );
        }

        @Override
        public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
            return Collections.emptySet();
        }

    }

}
