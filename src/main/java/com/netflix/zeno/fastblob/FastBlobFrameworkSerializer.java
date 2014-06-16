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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.state.FastBlobTypeSerializationState;
import com.netflix.zeno.serializer.FrameworkSerializer;

/**
 * Defines the binary serialized representation for each of the "Zeno native" elements in a FastBlob
 *
 * @author dkoszewnik
 *
 */
public class FastBlobFrameworkSerializer extends FrameworkSerializer<FastBlobSerializationRecord> {

    public static final int NULL_FLOAT_BITS = Float.floatToIntBits(Float.NaN) + 1;
    public static final long NULL_DOUBLE_BITS = Double.doubleToLongBits(Double.NaN) + 1;

    public FastBlobFrameworkSerializer(FastBlobStateEngine framework) {
        super(framework);
    }

    /**
     * Serialize a primitive element
     */
    @Override
    public void serializePrimitive(FastBlobSerializationRecord rec, String fieldName, Object value) {
        if (value == null) {
            return;
        }

        if (value instanceof Integer) {
            serializePrimitive(rec, fieldName, ((Integer) value).intValue());
        } else if (value instanceof Long) {
            serializePrimitive(rec, fieldName, ((Long) value).longValue());
        } else if (value instanceof Float) {
            serializePrimitive(rec, fieldName, ((Float) value).floatValue());
        } else if (value instanceof Double) {
            serializePrimitive(rec, fieldName, ((Double) value).doubleValue());
        } else if (value instanceof Boolean) {
            serializePrimitive(rec, fieldName, ((Boolean) value).booleanValue());
        } else if (value instanceof String) {
            serializeString(rec, fieldName, (String) value);
        } else if (value instanceof byte[]){
            serializeBytes(rec, fieldName, (byte[]) value);
        } else {
            throw new RuntimeException("Primitive type " + value.getClass().getSimpleName() + " not supported!");
        }

    }

    /**
     * Serialize a string as the UTF-8 value
     */
    public void serializeString(FastBlobSerializationRecord rec, String fieldName, String value) {
        if(value == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.STRING)
            throw new IllegalArgumentException("Attempting to serialize a String as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        writeString(value, fieldBuffer);
    }

    /**
     * Serialize an integer, use zig-zag encoding to (probably) get a small positive value, then encode the result as a variable-byte integer.
     */
    @Override
    public void serializePrimitive(FastBlobSerializationRecord rec, String fieldName, int value) {
        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.INT && fieldType != FieldType.LONG)
            throw new IllegalArgumentException("Attempting to serialize an int as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");


        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        // zig zag encoding
        VarInt.writeVInt(fieldBuffer, (value << 1) ^ (value >> 31));
    }

    /**
     * Serialize a long, use zig-zag encoding to (probably) get a small positive value, then encode the result as a variable-byte long.
     */
    @Override
    public void serializePrimitive(FastBlobSerializationRecord rec, String fieldName, long value) {
        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.LONG)
            throw new IllegalArgumentException("Attempting to serialize a long as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");


        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        // zig zag encoding
        VarInt.writeVLong(fieldBuffer, (value << 1) ^ (value >> 63));
    }

    /**
     * Serialize a float into 4 consecutive bytes
     */
    @Override
    public void serializePrimitive(FastBlobSerializationRecord rec, String fieldName, float value) {
        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.FLOAT) {
            throw new IllegalArgumentException("Attempting to serialize a float as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");
        }

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);
        int intBits = Float.floatToIntBits(value);
        writeFixedLengthInt(fieldBuffer, intBits);
    }

    /**
     * Serialize a special 4-byte long sequence indicating a null Float value.
     */
    public static void writeNullFloat(final ByteDataBuffer fieldBuffer) {
        writeFixedLengthInt(fieldBuffer, NULL_FLOAT_BITS);
    }

    /**
     * Write 4 consecutive bytes
     */
    private static void writeFixedLengthInt(ByteDataBuffer fieldBuffer, int intBits) {
        fieldBuffer.write((byte) (intBits >>> 24));
        fieldBuffer.write((byte) (intBits >>> 16));
        fieldBuffer.write((byte) (intBits >>> 8));
        fieldBuffer.write((byte) (intBits));
    }

    /**
     * Serialize a double into 8 consecutive bytes
     */
    @Override
    public void serializePrimitive(FastBlobSerializationRecord rec, String fieldName, double value) {
        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.DOUBLE)
            throw new IllegalArgumentException("Attempting to serialize a double as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);
        long intBits = Double.doubleToLongBits(value);
        writeFixedLengthLong(fieldBuffer, intBits);
    }

    /**
     * Serialize a special 8-byte long sequence indicating a null Double value.
     */
    public static void writeNullDouble(ByteDataBuffer fieldBuffer) {
        writeFixedLengthLong(fieldBuffer, NULL_DOUBLE_BITS);
    }

    /**
     * Write 8 consecutive bytes
     */
    private static void writeFixedLengthLong(ByteDataBuffer fieldBuffer, long intBits) {
        fieldBuffer.write((byte) (intBits >>> 56));
        fieldBuffer.write((byte) (intBits >>> 48));
        fieldBuffer.write((byte) (intBits >>> 40));
        fieldBuffer.write((byte) (intBits >>> 32));
        fieldBuffer.write((byte) (intBits >>> 24));
        fieldBuffer.write((byte) (intBits >>> 16));
        fieldBuffer.write((byte) (intBits >>> 8));
        fieldBuffer.write((byte) (intBits));
    }

    /**
     * Serialize a boolean as a single byte
     */
    @Override
    public void serializePrimitive(FastBlobSerializationRecord rec, String fieldName, boolean value) {
        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.BOOLEAN)
            throw new IllegalArgumentException("Attempting to serialize a boolean as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);
        byte byteValue = value ? (byte) 1 : (byte) 0;
        fieldBuffer.write(byteValue);
    }

    /**
     * Serialize a sequence of bytes
     */
    @Override
    public void serializeBytes(FastBlobSerializationRecord rec, String fieldName, byte[] value) {
        if(value == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.BYTES)
            throw new IllegalArgumentException("Attempting to serialize a byte array as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        for (int i = 0; i < value.length; i++) {
            fieldBuffer.write(value[i]);
        }
    }

    /**
     * Recursively call the framework to serialize the speicfied Object, then serialize the resulting ordinal as a variable-byte integer.
     */
    @Deprecated
    @Override
    public void serializeObject(FastBlobSerializationRecord rec, String fieldName, String typeName, Object obj) {
        int position = rec.getSchema().getPosition(fieldName);
        validateField(fieldName, position);
        serializeObject(rec, position, fieldName, typeName, obj);
    }

    private void validateField(String fieldName, int position) {
        if(position == -1) {
            throw new IllegalArgumentException("Attempting to serialize non existent field " + fieldName + ".");
        }
    }

    protected void serializeObject(FastBlobSerializationRecord rec, int position, String fieldName, String typeName, Object obj) {
        if(obj == null)
            return;

        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.OBJECT)
            throw new IllegalArgumentException("Attempting to serialize an Object as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        FastBlobTypeSerializationState<Object> typeSerializationState = ((FastBlobStateEngine) framework).getTypeSerializationState(typeName);

        int ordinal = typeSerializationState.add(obj, rec.getImageMembershipsFlags());

        VarInt.writeVInt(fieldBuffer, ordinal);
    }

    @Override
    public void serializeObject(FastBlobSerializationRecord rec, String fieldName, Object obj) {
        int position = rec.getSchema().getPosition(fieldName);
        validateField(fieldName, position);
        serializeObject(rec, position, fieldName, rec.getObjectType(fieldName), obj);
    }

    /**
     * Serialize a list.
     *
     * The framework is used to recursively serialize each of the list's elements, then
     * the ordinals are encoded as a sequence of variable-byte integers.
     */
    @Override
    public <T> void serializeList(FastBlobSerializationRecord rec, String fieldName, String typeName, Collection<T> collection) {
        if(collection == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.LIST && fieldType != FieldType.COLLECTION)
            throw new IllegalArgumentException("Attempting to serialize a List as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        FastBlobTypeSerializationState<Object> typeSerializationState = ((FastBlobStateEngine) framework).getTypeSerializationState(typeName);

        for (T obj : collection) {
            if(obj == null) {
                VarInt.writeVNull(fieldBuffer);
            } else {
                int ordinal = typeSerializationState.add(obj, rec.getImageMembershipsFlags());
                VarInt.writeVInt(fieldBuffer, ordinal);
            }
        }
    }

    /**
     * Serialize a set.
     *
     *  The framework is used to recursively serialize each of the set's elements, then
     *  the ordinals are encoded as a sequence of gap-encoded variable-byte integers.
     */
    @Override
    public <T> void serializeSet(FastBlobSerializationRecord rec, String fieldName, String typeName, Set<T> set) {
        if(set == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.SET && fieldType != FieldType.COLLECTION)
            throw new IllegalArgumentException("Attempting to serialize a Set as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);
        FastBlobTypeSerializationState<Object> typeSerializationState = ((FastBlobStateEngine) framework).getTypeSerializationState(typeName);
        int setOrdinals[] = new int[set.size()];

        int i = 0;
        for (T obj : set) {
            if(obj == null) {
                setOrdinals[i++] = -1;
            } else {
                setOrdinals[i++] = typeSerializationState.add(obj, rec.getImageMembershipsFlags());
            }
        }

        if(setOrdinals.length > i)
            setOrdinals = Arrays.copyOf(setOrdinals, i);

        Arrays.sort(setOrdinals);

        int currentOrdinal = 0;

        for (i = 0; i < setOrdinals.length; i++) {
            if(setOrdinals[i] == -1) {
                VarInt.writeVNull(fieldBuffer);
            } else {
                VarInt.writeVInt(fieldBuffer, setOrdinals[i] - currentOrdinal);
                currentOrdinal = setOrdinals[i];
            }
        }
    }

    /**
     * Serialize a Map.
     *
     * The framework is used to recursively serialize the map's keys and values, then
     * the Map's entries are each encoded as a variable-byte integer for the key's ordinal, and a gap-encoded variable-byte integer for the value's ordinal.
     */
    @Override
    public <K, V> void serializeMap(FastBlobSerializationRecord rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> map) {
        if(map == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.MAP)
            throw new IllegalArgumentException("Attempting to serialize a Map as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);
        FastBlobTypeSerializationState<K> keySerializationState = ((FastBlobStateEngine) framework).getTypeSerializationState(keyTypeName);
        FastBlobTypeSerializationState<V> valueSerializationState = ((FastBlobStateEngine) framework).getTypeSerializationState(valueTypeName);
        long mapEntries[] = new long[map.size()];

        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            int keyOrdinal = -1;
            int valueOrdinal = -1;

            if(entry.getKey() != null)
                keyOrdinal = keySerializationState.add(entry.getKey(), rec.getImageMembershipsFlags());
            if(entry.getValue() != null)
                valueOrdinal = valueSerializationState.add(entry.getValue(), rec.getImageMembershipsFlags());

            mapEntries[i++] = ((long)valueOrdinal << 32) | (keyOrdinal & 0xFFFFFFFFL);
        }

        if(mapEntries.length > i)
            mapEntries = Arrays.copyOf(mapEntries, i);

        Arrays.sort(mapEntries);

        int currentValueOrdinal = 0;

        for (i = 0; i < mapEntries.length ; i++) {
            int keyOrdinal = (int) mapEntries[i];
            int valueOrdinal = (int) (mapEntries[i] >> 32);

            if(keyOrdinal == -1)
                VarInt.writeVNull(fieldBuffer);
            else
                VarInt.writeVInt(fieldBuffer, keyOrdinal);

            if(valueOrdinal == -1) {
                VarInt.writeVNull(fieldBuffer);
            } else {
                VarInt.writeVInt(fieldBuffer, valueOrdinal - currentValueOrdinal);
                currentValueOrdinal = valueOrdinal;
            }
        }
    }


    /**
     * Encode a String as a series of VarInts, one per character.<p/>
     *
     * @param str
     * @param out
     * @return
     * @throws IOException
     */
    protected void writeString(String str, ByteDataBuffer out) {
        for(int i=0;i<str.length();i++) {
            VarInt.writeVInt(out, str.charAt(i));
        }
    }

}
