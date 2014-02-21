/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.zeno.flatblob;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.serializer.FrameworkSerializer;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FlatBlobFrameworkSerializer extends FrameworkSerializer<FlatBlobSerializationRecord> {

    static final int NULL_FLOAT_BITS = Float.floatToIntBits(Float.NaN) + 1;
    static final long NULL_DOUBLE_BITS = Double.doubleToLongBits(Double.NaN) + 1;

    private final FastBlobStateEngine stateEngine;
    private final ThreadLocal<Map<String, FlatBlobSerializationRecord>> cachedSerializationRecords;


    public FlatBlobFrameworkSerializer(FlatBlobSerializationFramework flatBlobFramework, FastBlobStateEngine stateEngine) {
        super(flatBlobFramework);
        this.stateEngine = stateEngine;
        this.cachedSerializationRecords = new ThreadLocal<Map<String, FlatBlobSerializationRecord>>();
    }

    /**
     * Serialize a primitive element
     */
    @Override
    public void serializePrimitive(FlatBlobSerializationRecord rec, String fieldName, Object value) {
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
     * Serialize an integer, use zig-zag encoding to (probably) get a small positive value, then encode the result as a variable-byte integer.
     */
    @Override
    public void serializePrimitive(FlatBlobSerializationRecord rec, String fieldName, int value) {
        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldName);

        // zig zag encoding
        VarInt.writeVInt(fieldBuffer, (value << 1) ^ (value >> 31));
    }

    /**
     * Serialize a long, use zig-zag encoding to (probably) get a small positive value, then encode the result as a variable-byte long.
     */
    @Override
    public void serializePrimitive(FlatBlobSerializationRecord rec, String fieldName, long value) {
        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldName);

        // zig zag encoding
        VarInt.writeVLong(fieldBuffer, (value << 1) ^ (value >> 63));
    }

    /**
     * Serialize a float into 4 consecutive bytes
     */
    @Override
    public void serializePrimitive(FlatBlobSerializationRecord rec, String fieldName, float value) {
        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldName);
        int intBits = Float.floatToIntBits(value);
        writeFixedLengthInt(fieldBuffer, intBits);
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
    public void serializePrimitive(FlatBlobSerializationRecord rec, String fieldName, double value) {
        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldName);
        long intBits = Double.doubleToLongBits(value);
        writeFixedLengthLong(fieldBuffer, intBits);
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
    public void serializePrimitive(FlatBlobSerializationRecord rec, String fieldName, boolean value) {
        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldName);
        byte byteValue = value ? (byte) 1 : (byte) 0;
        fieldBuffer.write(byteValue);
    }

    private void serializeString(FlatBlobSerializationRecord rec, String fieldName, String value) {
        if(value == null)
            return;

        writeString(value, rec.getFieldBuffer(fieldName));
    }

    @Override
    public void serializeBytes(FlatBlobSerializationRecord rec, String fieldName, byte[] value) {
        if(value == null)
            return;

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldName);

        for (int i = 0; i < value.length; i++) {
            fieldBuffer.write(value[i]);
        }
    }
    
    /*
     * @Deprecated instead use serializeObject(FlatBlobSerializationRecord rec, String fieldName, Object obj)
     * 
     */
    @Deprecated
    @Override
    public void serializeObject(FlatBlobSerializationRecord rec, String fieldName, String typeName, Object obj) {
        int fieldPosition = rec.getSchema().getPosition(fieldName);
        validateField(fieldName, fieldPosition);
        serializeObject(rec, fieldPosition, typeName, obj);
    }

    private void validateField(String fieldName, int fieldPosition) {
        if(fieldPosition == -1) {
            throw new IllegalArgumentException("Attempting to serialize non existent field " + fieldName + ".");            
        }
    }

    private void serializeObject(FlatBlobSerializationRecord rec, int fieldPosition, String typeName, Object obj) {
        if(obj == null)
            return;
        
        int ordinal = findOrdinalInStateEngine(typeName, obj);

        FlatBlobSerializationRecord subRecord = getSerializationRecord(typeName);
        framework.getSerializer(typeName).serialize(obj, subRecord);

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldPosition);
        VarInt.writeVInt(fieldBuffer, ordinal);
        VarInt.writeVInt(fieldBuffer, subRecord.sizeOfData());
        subRecord.writeDataTo(fieldBuffer);
    }
    
    @Override
    public void serializeObject(FlatBlobSerializationRecord rec, String fieldName, Object obj) {
        int fieldPosition = rec.getSchema().getPosition(fieldName);
        validateField(fieldName, fieldPosition);
        serializeObject(rec, fieldPosition, rec.getSchema().getObjectType(fieldName), obj);
    }

    @Override
    public <T> void serializeList(FlatBlobSerializationRecord rec, String fieldName, String typeName, Collection<T> obj) {
        if(obj == null)
            return;

        NFTypeSerializer<Object> elementSerializer = framework.getSerializer(typeName);

        int fieldPosition = rec.getSchema().getPosition(fieldName);

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldPosition);

        FlatBlobSerializationRecord subRecord = getSerializationRecord(typeName);

        for(T t : obj) {
            if(t == null) {
                VarInt.writeVNull(fieldBuffer);
            } else {
                int ordinal = findOrdinalInStateEngine(typeName, t);
                elementSerializer.serialize(t, subRecord);

                VarInt.writeVInt(fieldBuffer, ordinal);
                VarInt.writeVInt(fieldBuffer, subRecord.sizeOfData());

                subRecord.writeDataTo(fieldBuffer);
                subRecord.reset();
            }
        }
    }

    @Override
    public <T> void serializeSet(FlatBlobSerializationRecord rec, String fieldName, String typeName, Set<T> set) {
        if(set == null)
            return;

        FastBlobTypeDeserializationState<Object> typeDeserializationState = stateEngine.getTypeDeserializationState(typeName);

        int fieldPosition = rec.getSchema().getPosition(fieldName);

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldPosition);

        int setOrdinals[] = new int[set.size()];
        Object unidentifiedSetObjects[] = null;

        int i = 0;
        for (T obj : set) {
            if(obj == null) {
                setOrdinals[i++] = -1;
            } else {
                setOrdinals[i] = typeDeserializationState.find(obj);
                if(setOrdinals[i] == -1) {
                    if(unidentifiedSetObjects == null)
                        unidentifiedSetObjects = new Object[set.size()];
                    unidentifiedSetObjects[i] = obj;
                    setOrdinals[i] = Integer.MIN_VALUE;
                }
                i++;
            }
        }

        Arrays.sort(setOrdinals);

        FlatBlobSerializationRecord subRecord = getSerializationRecord(typeName);

        int currentOrdinal = 0;

        for(i=0;i<setOrdinals.length;i++) {
            if(setOrdinals[i] == -1) {
                VarInt.writeVNull(fieldBuffer);
                VarInt.writeVNull(fieldBuffer);
            } else {
                if(setOrdinals[i]  == Integer.MIN_VALUE) {
                    Object element = unidentifiedSetObjects[i];
                    framework.getSerializer(typeName).serialize(element, subRecord);
                    VarInt.writeVNull(fieldBuffer);
                } else {
                    Object element = typeDeserializationState.get(setOrdinals[i]);
                    framework.getSerializer(typeName).serialize(element, subRecord);

                    VarInt.writeVInt(fieldBuffer, setOrdinals[i] - currentOrdinal);
                    currentOrdinal = setOrdinals[i];
                }

                VarInt.writeVInt(fieldBuffer, subRecord.sizeOfData());

                subRecord.writeDataTo(fieldBuffer);
                subRecord.reset();
            }
        }
    }

    @Override
    public <K, V> void serializeMap(FlatBlobSerializationRecord rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> map) {
        if(map == null)
            return;

        FastBlobTypeDeserializationState<Object> keyDeserializationState = stateEngine.getTypeDeserializationState(keyTypeName);
        FastBlobTypeDeserializationState<Object> valueDeserializationState = stateEngine.getTypeDeserializationState(valueTypeName);

        int fieldPosition = rec.getSchema().getPosition(fieldName);

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(fieldPosition);

        FlatBlobSerializationRecord keyRecord = getSerializationRecord(keyTypeName);
        FlatBlobSerializationRecord valueRecord = getSerializationRecord(valueTypeName);


        long mapEntries[] = new long[map.size()];

        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            int keyOrdinal = -1;
            int valueOrdinal = -1;

            if(entry.getKey() != null)
                keyOrdinal = keyDeserializationState.find(entry.getKey());
            if(entry.getValue() != null)
                valueOrdinal = valueDeserializationState.find(entry.getValue());

            mapEntries[i++] = ((long)valueOrdinal << 32) | (keyOrdinal & 0xFFFFFFFFL);
        }

        if(mapEntries.length > i) {
            mapEntries = Arrays.copyOf(mapEntries, i);
            throw new RuntimeException("This should not happen."); ///TODO: Remove this sanity check.
        }

        Arrays.sort(mapEntries);

        int currentValueOrdinal = 0;

        for(i=0;i<mapEntries.length;i++) {
            int keyOrdinal = (int) mapEntries[i];
            int valueOrdinal = (int) (mapEntries[i] >> 32);

            if(keyOrdinal == -1) {
                VarInt.writeVNull(fieldBuffer);
            } else {
                Object key = keyDeserializationState.get(keyOrdinal);
                keyRecord.reset();
                framework.getSerializer(keyTypeName).serialize(key, keyRecord);
                VarInt.writeVInt(fieldBuffer, keyOrdinal);
                VarInt.writeVInt(fieldBuffer, keyRecord.sizeOfData());
                keyRecord.writeDataTo(fieldBuffer);
            }


            if(valueOrdinal == -1) {
                VarInt.writeVNull(fieldBuffer);
            } else {
                Object value = valueDeserializationState.get(valueOrdinal);
                valueRecord.reset();
                framework.getSerializer(valueTypeName).serialize(value, valueRecord);
                VarInt.writeVInt(fieldBuffer, valueOrdinal - currentValueOrdinal);
                VarInt.writeVInt(fieldBuffer, valueRecord.sizeOfData());
                valueRecord.writeDataTo(fieldBuffer);
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
    private void writeString(String str, ByteDataBuffer out) {
        for(int i=0;i<str.length();i++) {
            VarInt.writeVInt(out, str.charAt(i));
        }
    }


    private int findOrdinalInStateEngine(String typeName, Object obj) {
        FastBlobTypeDeserializationState<Object> typeDeserializationState = stateEngine.getTypeDeserializationState(typeName);
        int ordinal = typeDeserializationState.find(obj);
        return ordinal;
    }

    FlatBlobSerializationRecord getSerializationRecord(String type) {
        Map<String, FlatBlobSerializationRecord> cachedSerializationRecords = this.cachedSerializationRecords.get();
        if(cachedSerializationRecords == null) {
            cachedSerializationRecords = new HashMap<String, FlatBlobSerializationRecord>();
            this.cachedSerializationRecords.set(cachedSerializationRecords);
        }

        FlatBlobSerializationRecord rec = cachedSerializationRecords.get(type);
        if(rec == null) {
            rec = new FlatBlobSerializationRecord(framework.getSerializer(type).getFastBlobSchema());
            cachedSerializationRecords.put(type, rec);
        }
        rec.reset();
        return rec;
    }

}
