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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.record.schema.FieldDefinition;
import com.netflix.zeno.fastblob.record.schema.TypedFieldDefinition;

public class FastBlobSchemaTest {

    private FastBlobSchema schema;

    @Before
    public void setUp() {
        schema = new FastBlobSchema("test", 3);

        schema.addField("field1", new FieldDefinition(FieldType.INT));
        schema.addField("field2", new TypedFieldDefinition(FieldType.OBJECT, "Field2"));
        schema.addField("field3", new FieldDefinition(FieldType.FLOAT));
    }

    @Test
    public void retainsFieldDescriptions() {
        Assert.assertEquals(FieldType.INT, schema.getFieldType("field1"));
        Assert.assertEquals(FieldType.OBJECT, schema.getFieldType("field2"));
        Assert.assertEquals(FieldType.FLOAT, schema.getFieldType("field3"));
    }

    @Test
    public void serializesAndDeserializes() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        schema.writeTo(new DataOutputStream(os));

        FastBlobSchema deserialized = FastBlobSchema.readFrom(new DataInputStream(new ByteArrayInputStream(os.toByteArray())));

        Assert.assertEquals(3, deserialized.numFields());
        Assert.assertEquals(FieldType.INT, deserialized.getFieldType("field1"));
        Assert.assertEquals(FieldType.OBJECT, deserialized.getFieldType("field2"));
        Assert.assertEquals(FieldType.FLOAT, deserialized.getFieldType("field3"));
    }


    @Test
    public void testEquals() throws IOException {
        FastBlobSchema otherSchema = new FastBlobSchema("test", 3);

        otherSchema.addField("field1", new FieldDefinition(FieldType.INT));
        otherSchema.addField("field2", new TypedFieldDefinition(FieldType.OBJECT, "Field2"));
        otherSchema.addField("field3", new FieldDefinition(FieldType.FLOAT));

        Assert.assertTrue(otherSchema.equals(schema));


        FastBlobSchema anotherSchema = new FastBlobSchema("test", 3);

        anotherSchema.addField("field1", new FieldDefinition(FieldType.INT));
        anotherSchema.addField("field2", new TypedFieldDefinition(FieldType.OBJECT, "Field2"));
        anotherSchema.addField("field3", new FieldDefinition(FieldType.INT));

        Assert.assertFalse(anotherSchema.equals(schema));
    }



}
