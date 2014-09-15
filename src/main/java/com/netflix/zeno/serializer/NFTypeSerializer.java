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
package com.netflix.zeno.serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.record.schema.FieldDefinition;
import com.netflix.zeno.fastblob.record.schema.MapFieldDefinition;
import com.netflix.zeno.fastblob.record.schema.TypedFieldDefinition;
/**
 * The NFTypeSerializer is used to represent a hierarchy of Objects contained in a POJO Object model.<p/>
 *
 * Using a set of NFTypeSerializers to define an Object hierarchy allows for the definition of
 * semantically and structurally independent operations over that data.
 *
 * Check out the <a href="https://github.com/Netflix/zeno/wiki">Zeno documentation</a> section
 * <a href="https://github.com/Netflix/zeno/wiki/Defining-an-object-model">defining an object model</a> for details about how
 * to define your data model with NFTypeSerializers.
 *
 * @param <T> - The type of object this serializer serializes
 */
public abstract class NFTypeSerializer<T> {

    private final String schemaName;
    private FastBlobSchema schema;

    protected SerializationFramework serializationFramework;

    public NFTypeSerializer(String schemaName) {
        this.schemaName = schemaName;
    }

    public void serialize(T value, NFSerializationRecord rec) {
        /// automatically track the schemas.
        FastBlobSchema existingSchema = rec.getSchema();
        rec.setSchema(getFastBlobSchema());
        doSerialize(value, rec);
        rec.setSchema(existingSchema);
    }

    public T deserialize(NFDeserializationRecord rec) {
        return doDeserialize(rec);
    }

    protected abstract void doSerialize(T value, NFSerializationRecord rec);

    protected abstract T doDeserialize(NFDeserializationRecord rec);

    protected abstract FastBlobSchema createSchema();

    public abstract Collection<NFTypeSerializer<?>> requiredSubSerializers();

    public Collection<NFTypeSerializer<?>> requiredSubSerializers(FastBlobSchemaField[] fields) {
        Collection<NFTypeSerializer<?>> requiredSubSerializers = new HashSet<NFTypeSerializer<?>>();
        for(FastBlobSchemaField field : fields) {
            if(field.typeSerializer != null) {
                requiredSubSerializers.add(field.typeSerializer);
            }
        }
        return requiredSubSerializers;
    }

    @SuppressWarnings("unchecked")
    protected void serializePrimitive(NFSerializationRecord rec, String fieldName, Object value) {
        serializationFramework.getFrameworkSerializer().serializePrimitive(rec, fieldName, value);
    }

    @SuppressWarnings("unchecked")
    protected void serializeBytes(NFSerializationRecord rec, String fieldName, byte[] value) {
        serializationFramework.getFrameworkSerializer().serializeBytes(rec, fieldName, value);
    }

    /**
     * @Deprecated instead use serializeObject(NFSerializationRecord rec, String fieldName, Object obj)
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    protected void serializeObject(NFSerializationRecord rec, String fieldName, String typeName, Object obj) {
        serializationFramework.getFrameworkSerializer().serializeObject(rec, fieldName, typeName, obj);
    }

    @SuppressWarnings("unchecked")
    protected void serializeObject(NFSerializationRecord rec, String fieldName, Object obj) {
        serializationFramework.getFrameworkSerializer().serializeObject(rec, fieldName, obj);
    }

    /**
     * @deprecated use instead deserializeObject(NFDeserializationRecord rec, String fieldName);
     */
    @Deprecated
    @SuppressWarnings("unchecked")
    protected <X> X deserializeObject(NFDeserializationRecord rec, String typeName, String fieldName) {
        return (X) serializationFramework.getFrameworkDeserializer().deserializeObject(rec, fieldName, typeName, null);
    }

    @SuppressWarnings("unchecked")
    protected <X> X deserializeObject(NFDeserializationRecord rec, String fieldName) {
        return (X) serializationFramework.getFrameworkDeserializer().deserializeObject(rec, fieldName, null);
    }

    public FastBlobSchema getFastBlobSchema() {
        if(schema == null)
            schema = createSchema();
        return schema;
    }

    public String getName() {
        return schemaName;
    }

    public void setSerializationFramework(SerializationFramework framework) {
        if(serializationFramework != null && serializationFramework != framework) {
            throw new RuntimeException("Should not be replacing an existing SerializationFramework with a different one!  This can cause bugs which are difficult to track down!");
        }
        this.serializationFramework = framework;
    }

    /**
     * @deprecated use instead field(String name, String objectType);
     */
    @Deprecated
    protected FastBlobSchemaField field(String name) {
        return field(name, FieldType.OBJECT);
    }

    protected FastBlobSchemaField field(String name, String objectType) {
        FastBlobSchemaField field = new FastBlobSchemaField();
        field.name = name;
        field.type = new TypedFieldDefinition(FieldType.OBJECT, objectType);
        return field;
    }

    protected FastBlobSchemaField field(String name, NFTypeSerializer<?> typeSerializer) {
        FastBlobSchemaField field = new FastBlobSchemaField();
        field.name = name;
        field.type = new TypedFieldDefinition(FieldType.OBJECT, typeSerializer.getName());
        field.typeSerializer = typeSerializer;
        return field;
    }

    protected FastBlobSchemaField field(String name, FieldType fieldType) {
        FastBlobSchemaField field = new FastBlobSchemaField();
        field.name = name;
        field.type = new FieldDefinition(fieldType);
        return field;
    }

    protected FastBlobSchemaField listField(String name, String elementType) {
        FastBlobSchemaField field = new FastBlobSchemaField();
        field.name = name;
        field.type = new TypedFieldDefinition(FieldType.LIST, elementType);
        return field;
    }

    protected FastBlobSchemaField setField(String name, String elementType) {
        FastBlobSchemaField field = new FastBlobSchemaField();
        field.name = name;
        field.type = new TypedFieldDefinition(FieldType.SET, elementType);
        return field;
    }

    protected FastBlobSchemaField mapField(String name, String keyType, String valueType) {
        FastBlobSchemaField field = new FastBlobSchemaField();
        field.name = name;
        field.type = new MapFieldDefinition(keyType, valueType);
        return field;
    }

    protected FastBlobSchema schema(FastBlobSchemaField... fields) {
        FastBlobSchema schema = new FastBlobSchema(schemaName, fields.length);
        for(FastBlobSchemaField field : fields) {
            schema.addField(field.name, field.type);
        }
        return schema;
    }

    protected List<NFTypeSerializer<?>> serializers(NFTypeSerializer<?>... serializers) {
        List<NFTypeSerializer<?>> list = new ArrayList<NFTypeSerializer<?>>();
        for (NFTypeSerializer<?> s : serializers) {
            list.add(s);
        }
        return list;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected boolean deserializeBoolean(NFDeserializationRecord rec, String field) {
        FrameworkDeserializer frameworkSerializer = serializationFramework.getFrameworkDeserializer();
        return frameworkSerializer.deserializePrimitiveBoolean(rec, field);
    }

    @SuppressWarnings("unchecked")
    protected boolean deserializeBoolean(NFDeserializationRecord rec, String field, boolean defaultVal) {
        Boolean recObj = serializationFramework.getFrameworkDeserializer().deserializeBoolean(rec, field);
        if (recObj == null)
            return defaultVal;
        return recObj.booleanValue();
    }

    @SuppressWarnings("unchecked")
    protected Integer deserializeInteger(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializeInteger(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected int deserializePrimitiveInt(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializePrimitiveInt(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected Long deserializeLong(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializeLong(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected long deserializePrimitiveLong(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializePrimitiveLong(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected Float deserializeFloat(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializeFloat(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected float deserializePrimitiveFloat(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializePrimitiveFloat(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected Double deserializeDouble(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializeDouble(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected double deserializePrimitiveDouble(NFDeserializationRecord rec, String fieldName) {
        return serializationFramework.getFrameworkDeserializer().deserializePrimitiveDouble(rec, fieldName);
    }

    @SuppressWarnings("unchecked")
    protected String deserializePrimitiveString(NFDeserializationRecord rec, String field) {
        return serializationFramework.getFrameworkDeserializer().deserializeString(rec, field);
    }

    @SuppressWarnings("unchecked")
    protected byte[] deserializeBytes(NFDeserializationRecord rec, String field) {
        return serializationFramework.getFrameworkDeserializer().deserializeBytes(rec, field);
    }

    public SerializationFramework getSerializationFramework() {
        return serializationFramework;
    }

    public static class FastBlobSchemaField {
        public String name;
        public FieldDefinition type;
        public NFTypeSerializer<?> typeSerializer;
    }

}
