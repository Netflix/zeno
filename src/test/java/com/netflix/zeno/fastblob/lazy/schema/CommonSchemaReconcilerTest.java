package com.netflix.zeno.fastblob.lazy.schema;

import com.netflix.zeno.fastblob.FastBlobFrameworkDeserializer;
import com.netflix.zeno.fastblob.FastBlobFrameworkSerializer;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.TypedFieldDefinition;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.record.schema.FieldDefinition;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CommonSchemaReconcilerTest {

    private FastBlobSchema schema1;
    private FastBlobSchema schema2;

    @Before
    public void setUp() {
        schema1 = new FastBlobSchema("schema1", 3);
        schema1.addField("field1", new FieldDefinition(FieldType.STRING));
        schema1.addField("field2", new FieldDefinition(FieldType.BOOLEAN));
        schema1.addField("field3", new FieldDefinition(FieldType.BYTES));

        schema2 = new FastBlobSchema("schema1", 3);
        schema2.addField("field3", new FieldDefinition(FieldType.BYTES));
        schema2.addField("field1", new FieldDefinition(FieldType.STRING));
        schema2.addField("field4", new FieldDefinition(FieldType.DOUBLE));
    }


    @Test
    public void derivesCommonSchemas() {
        FastBlobSchema commonSchema = CommonSchemaReconciler.deriveCommonSchema(schema1, schema2);

        Assert.assertEquals(2, commonSchema.numFields());
        Assert.assertEquals(new FieldDefinition(FieldType.BYTES), commonSchema.getFieldDefinition("field3"));
        Assert.assertEquals(new FieldDefinition(FieldType.STRING), commonSchema.getFieldDefinition("field1"));
    }

    @Test
    public void throwsExceptionIfSchemasHaveSameNameButDifferentFieldDefinitions() {
        FastBlobSchema schema1 = new FastBlobSchema("test", 1);
        schema1.addField("field1", new TypedFieldDefinition(FieldType.LIST, "Strings"));

        FastBlobSchema schema2 = new FastBlobSchema("test2", 1);
        schema2.addField("field1", new FieldDefinition(FieldType.BOOLEAN));

        try {
            CommonSchemaReconciler.deriveCommonSchema(schema1, schema2);
            Assert.fail("Should have thrown exception -- schema field1 is incompatible");
        } catch(Exception expected) { }
    }

    @Test
    public void convertDifferentSchemasToCommonSchemaAndDataShouldBeEquivalent() {
        FastBlobFrameworkSerializer serializer = new FastBlobFrameworkSerializer(null);

        FastBlobSerializationRecord rec1 = new FastBlobSerializationRecord(schema1);
        serializer.serializeString(rec1, "field1", "StringValue");
        serializer.serializePrimitive(rec1, "field2", Boolean.TRUE);
        serializer.serializeBytes(rec1, "field3", new byte[] { 1, 2, 3 });

        FastBlobSerializationRecord rec2 = new FastBlobSerializationRecord(schema2);
        serializer.serializeBytes(rec2, "field3", new byte[] { 1, 2, 3});
        serializer.serializeString(rec2, "field1", "StringValue");
        serializer.serializePrimitive(rec2, "field4", 1.2345D);

        FastBlobSchema commonSchema = CommonSchemaReconciler.deriveCommonSchema(schema1, schema2);

        FastBlobDeserializationRecord deserializationRec1 = createDeserializationRecord(rec1);
        FastBlobDeserializationRecord deserializationRec2 = createDeserializationRecord(rec2);

        ByteDataBuffer convertedBuf1 = new ByteDataBuffer();
        ByteDataBuffer convertedBuf2 = new ByteDataBuffer();

        CommonSchemaReconciler.swapRecordSchema(deserializationRec1, commonSchema, convertedBuf1);
        CommonSchemaReconciler.swapRecordSchema(deserializationRec2, commonSchema, convertedBuf2);

        FastBlobDeserializationRecord convertedRec1 = new FastBlobDeserializationRecord(commonSchema, convertedBuf1.getUnderlyingArray());
        int size1 = convertedRec1.position(0);

        FastBlobDeserializationRecord convertedRec2 = new FastBlobDeserializationRecord(commonSchema, convertedBuf2.getUnderlyingArray());
        int size2 = convertedRec2.position(0);

        Assert.assertEquals(size1, size2);

        for(int i=0;i<size1;i++) {
            Assert.assertEquals(convertedRec1.getByteData().get(i), convertedRec2.getByteData().get(i));
        }
    }

    @Test
    public void convertSchemaWithAddedAndRemovedFields() {
        FastBlobFrameworkSerializer serializer = new FastBlobFrameworkSerializer(null);

        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(schema1);
        serializer.serializeString(rec, "field1", "StringValue");
        serializer.serializePrimitive(rec, "field2", Boolean.TRUE);
        serializer.serializeBytes(rec, "field3", new byte[] { 1, 2, 3 });

        FastBlobDeserializationRecord deserializationRec = createDeserializationRecord(rec);

        ByteDataBuffer convertedBuf = new ByteDataBuffer();

        CommonSchemaReconciler.swapRecordSchema(deserializationRec, schema2, convertedBuf);

        FastBlobDeserializationRecord swappedDeserializationRec = new FastBlobDeserializationRecord(schema2, convertedBuf.getUnderlyingArray());
        swappedDeserializationRec.position(0);

        FastBlobFrameworkDeserializer deserializer = new FastBlobFrameworkDeserializer(null);

        Assert.assertEquals("StringValue", deserializer.deserializeString(swappedDeserializationRec, "field1"));
        Assert.assertArrayEquals(new byte[] { 1, 2, 3}, deserializer.deserializeBytes(swappedDeserializationRec, "field3"));
        Assert.assertNull(deserializer.deserializeDouble(swappedDeserializationRec, "field4"));
    }

    private FastBlobDeserializationRecord createDeserializationRecord(FastBlobSerializationRecord rec) {
        ByteDataBuffer buf1 = new ByteDataBuffer();
        rec.writeDataTo(buf1);
        FastBlobDeserializationRecord deserializationRec = new FastBlobDeserializationRecord(rec.getSchema(), buf1.getUnderlyingArray());
        deserializationRec.position(0);
        return deserializationRec;
    }

}
