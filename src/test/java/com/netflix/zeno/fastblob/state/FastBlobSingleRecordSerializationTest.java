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
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.serializer.NFDeserializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FastBlobSingleRecordSerializationTest {

    private FastBlobStateEngine stateEngine;
    private FastBlobSerializationRecord rec;
    private FastBlobSchema schema;

    @Before
    public void setUp() {
        stateEngine = new FastBlobStateEngine(new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TestSchemaSerializer() };
            }
        });

        schema = stateEngine.getSerializer("TestType").getFastBlobSchema();

        rec = new FastBlobSerializationRecord(schema);
    }


    @Test
    public void testSerializationAndDeserialization() {
        PojoWithAllTypes testType = new PojoWithAllTypes();
        testType.boolField = true;
        testType.bytesField = new byte[] {7, 8 };
        testType.doubleField = 3.14159265359d;
        testType.floatField = 2.71828182845f;
        testType.intField = 324515;
        testType.longField = 23523452345624634L;
        testType.stringField = "Hello world!";

        NFTypeSerializer<PojoWithAllTypes> serializer = stateEngine.getSerializer("TestType");

        serializer.serialize(testType, rec);

        ByteDataBuffer buf = new ByteDataBuffer();

        rec.writeDataTo(buf);

        FastBlobDeserializationRecord deserializationRecord = new FastBlobDeserializationRecord(schema, buf.getUnderlyingArray());
        deserializationRecord.position(0);

        PojoWithAllTypes deserialized = serializer.deserialize(deserializationRecord);

        Assert.assertTrue(deserialized.boolField);
        Assert.assertTrue(Arrays.equals(new byte[] {7, 8}, deserialized.bytesField));
        Assert.assertEquals(testType.floatField, deserialized.floatField);
        Assert.assertEquals(testType.longField, deserialized.longField);
        Assert.assertEquals(testType.intField, deserialized.intField);
        Assert.assertEquals(testType.stringField, deserialized.stringField);
    }


    public class TestSchemaSerializer extends NFTypeSerializer<PojoWithAllTypes> {

        public TestSchemaSerializer() {
            super("TestType");
        }

        @Override
        public void doSerialize(PojoWithAllTypes value, NFSerializationRecord rec) {
            serializePrimitive(rec, "bool", value.boolField);
            serializePrimitive(rec, "bool", value.boolField);
            serializePrimitive(rec, "int", value.intField);
            serializePrimitive(rec, "long", value.longField);
            serializePrimitive(rec, "float", value.floatField);
            serializePrimitive(rec, "float", value.floatField);
            serializePrimitive(rec, "double", value.doubleField);
            serializePrimitive(rec, "string", value.stringField);
            serializePrimitive(rec, "bytes", value.bytesField);
        }

        @Override
        protected PojoWithAllTypes doDeserialize(NFDeserializationRecord rec) {
            PojoWithAllTypes type = new PojoWithAllTypes();

            type.boolField = deserializeBoolean(rec, "bool");
            type.intField = deserializeInteger(rec, "int");
            type.longField = deserializeLong(rec, "long");
            type.floatField = deserializeFloat(rec, "float");
            type.stringField = deserializePrimitiveString(rec, "string");
            type.bytesField = deserializeBytes(rec, "bytes");

            return type;
        }

        @Override
        protected FastBlobSchema createSchema() {
            return schema(
                    field("bool", FieldType.BOOLEAN),
                    field("int", FieldType.INT),
                    field("long", FieldType.LONG),
                    field("float", FieldType.FLOAT),
                    field("double", FieldType.DOUBLE),
                    field("string", FieldType.STRING),
                    field("bytes", FieldType.BYTES)
            );
        }

        @Override
        public Collection<NFTypeSerializer<?>> requiredSubSerializers() {
            return Collections.emptyList();
        }

    }
}
