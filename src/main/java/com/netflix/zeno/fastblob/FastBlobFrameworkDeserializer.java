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

import static com.netflix.zeno.fastblob.FastBlobFrameworkSerializer.NULL_DOUBLE_BITS;
import static com.netflix.zeno.fastblob.FastBlobFrameworkSerializer.NULL_FLOAT_BITS;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.serializer.FrameworkDeserializer;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.util.collections.CollectionImplementation;
import com.netflix.zeno.util.collections.MinimizedUnmodifiableCollections;
import com.netflix.zeno.util.collections.builder.ListBuilder;
import com.netflix.zeno.util.collections.builder.MapBuilder;
import com.netflix.zeno.util.collections.builder.SetBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 *
 * Defines the operations necessary to decode each of the "Zeno native" elements from FastBlob record fields.
 *
 * @author dkoszewnik
 *
 */
public class FastBlobFrameworkDeserializer extends FrameworkDeserializer<FastBlobDeserializationRecord> {

    private MinimizedUnmodifiableCollections minimizedCollections = new MinimizedUnmodifiableCollections(CollectionImplementation.JAVA_UTIL);


    public FastBlobFrameworkDeserializer(FastBlobStateEngine framework) {
        super(framework);
    }

    public void setCollectionImplementation(CollectionImplementation impl) {
        minimizedCollections = new MinimizedUnmodifiableCollections(impl);
    }

    /**
     * Read a boolean as a single byte.  Might be null.
     */
    @Override
    public Boolean deserializeBoolean(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        return byteData.get(fieldPosition) == (byte) 1 ? Boolean.TRUE : Boolean.FALSE;
    }

    /**
     * Read a boolean as a single byte.
     */
    @Override
    public boolean deserializePrimitiveBoolean(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        return byteData.get(fieldPosition) == (byte) 1;
    }


    /**
     * Read an integer as a variable-byte sequence.  After read, the value must be zig-zag decoded.  Might be null.
     */
    @Override
    public Integer deserializeInteger(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int value = VarInt.readVInt(byteData, fieldPosition);

        return Integer.valueOf((value >>> 1) ^ ((value << 31) >> 31));
    }

    /**
     * Read an integer as a variable-byte sequence.  After read, the value must be zig-zag decoded.
     */
    @Override
    public int deserializePrimitiveInt(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        int value = VarInt.readVInt(byteData, fieldPosition);

        return (value >>> 1) ^ ((value << 31) >> 31);
    }


    /**
     * Read a long as a variable-byte sequence.  After read, the value must be zig-zag decoded.  Might be null.
     */
    @Override
    public Long deserializeLong(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        long value = VarInt.readVLong(byteData, fieldPosition);

        return Long.valueOf((value >>> 1) ^ ((value << 63) >> 63));
    }

    /**
     * Read a long as a variable-byte sequence.  After read, the value must be zig-zag decoded.
     */
    @Override
    public long deserializePrimitiveLong(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        long value = VarInt.readVLong(byteData, fieldPosition);

        return (value >>> 1) ^ ((value << 63) >> 63);
    }

    /**
     * Read a float as a fixed-length sequence of 4 bytes.  Might be null.
     */
    @Override
    public Float deserializeFloat(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1)
            return null;

        int intBits = readIntBits(byteData, fieldPosition);

        if(intBits == NULL_FLOAT_BITS)
            return null;

        return Float.valueOf(Float.intBitsToFloat(intBits));
    }

    /**
     * Read a float as a fixed-length sequence of 4 bytes.
     */
    @Override
    public float deserializePrimitiveFloat(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        int intBits = readIntBits(byteData, fieldPosition);

        return Float.intBitsToFloat(intBits);
    }


    private int readIntBits(ByteData byteData, long fieldPosition) {
        int intBits = (byteData.get(fieldPosition++) & 0xFF) << 24;
        intBits |= (byteData.get(fieldPosition++) & 0xFF) << 16;
        intBits |= (byteData.get(fieldPosition++) & 0xFF) << 8;
        intBits |= (byteData.get(fieldPosition) & 0xFF);
        return intBits;
    }

    /**
     * Read a double as a fixed-length sequence of 8 bytes.  Might be null.
     */
    @Override
    public Double deserializeDouble(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1)
            return null;

        long longBits = readLongBits(byteData, fieldPosition);

        if(longBits == NULL_DOUBLE_BITS)
            return null;

        return Double.valueOf(Double.longBitsToDouble(longBits));
    }

    /**
     * Read a double as a fixed-length sequence of 8 bytes.
     */
    @Override
    public double deserializePrimitiveDouble(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        long longBits = readLongBits(byteData, fieldPosition);

        return Double.longBitsToDouble(longBits);
    }


    private long readLongBits(ByteData byteData, long fieldPosition) {
        long longBits = (long) (byteData.get(fieldPosition++) & 0xFF) << 56;
        longBits |= (long) (byteData.get(fieldPosition++) & 0xFF) << 48;
        longBits |= (long) (byteData.get(fieldPosition++) & 0xFF) << 40;
        longBits |= (long) (byteData.get(fieldPosition++) & 0xFF) << 32;
        longBits |= (long) (byteData.get(fieldPosition++) & 0xFF) << 24;
        longBits |= (byteData.get(fieldPosition++) & 0xFF) << 16;
        longBits |= (byteData.get(fieldPosition++) & 0xFF) << 8;
        longBits |= (byteData.get(fieldPosition) & 0xFF);
        return longBits;
    }

    /**
     * Read a String as UTF-8 encoded characters.  The length is encoded as a variable-byte integer.
     */
    @Override
    public String deserializeString(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        return readString(byteData, fieldPosition, length);
    }

    /**
     * Read a sequence of bytes directly from the stream.  The length is encoded as a variable-byte integer.
     */
    @Override
    public byte[] deserializeBytes(FastBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        byte data[] = new byte[length];

        for(int i=0;i<length;i++) {
            data[i] = byteData.get(fieldPosition++);
        }

        return data;
    }


    /**
     * Read an Object's ordinal reference as a variable-byte integer.  Use the framework to look up the Object by ordinal.
     */
    @Override
    public <T> T deserializeObject(FastBlobDeserializationRecord rec, String fieldName, Class<T> clazz) {
        long fieldPosition = rec.getPosition(fieldName);
        if (fieldPosition == -1)
            return null;
        return deserializeObject(rec, fieldPosition, rec.getObjectType(fieldName));
    }

    /**
     * @deprecated use instead deserializeObject(FlatBlobDeserializationRecord rec, String fieldName, Class<T> clazz);
     *
     * Read an Object's ordinal reference as a variable-byte integer.  Use the framework to look up the Object by ordinal.
     */
    @Deprecated
    @Override
    public <T> T deserializeObject(FastBlobDeserializationRecord rec, String fieldName, String typeName, Class<T> clazz) {
        long fieldPosition = rec.getPosition(fieldName);
        if (fieldPosition == -1)
            return null;
        return deserializeObject(rec, fieldPosition, typeName);
    }

    private <T> T deserializeObject(FastBlobDeserializationRecord rec, long fieldPosition, String typeName) {
        ByteData byteData = rec.getByteData();

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int ordinal = VarInt.readVInt(byteData, fieldPosition);

        FastBlobTypeDeserializationState<T> deserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(typeName);

        return deserializationState.get(ordinal);
    }

    /**
     * Read a List as a sequence of ordinals encoded as variable-byte integers.  Use the framework to look up each Object by it's ordinals.
     */
    @Override
    public <T> List<T> deserializeList(FastBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<T> itemSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = VarInt.countVarIntsInRange(byteData, fieldPosition, length);

        if(numElements == 0)
            return Collections.emptyList();

        ListBuilder<T> list = minimizedCollections.createListBuilder();
        list.builderInit(numElements);

        FastBlobTypeDeserializationState<T> elementDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(itemSerializer.getName());

        for(int i=0;i<numElements;i++) {
            if(VarInt.readVNull(byteData, fieldPosition)) {
                list.builderSet(i, null);
                fieldPosition += 1;
            } else {
                int ordinal = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(ordinal);
                T element = elementDeserializationState.get(ordinal);

                if(element != null)
                    list.builderSet(i, element);
            }
        }

        return minimizedCollections.minimizeList(list.builderFinish());
    }

    /**
     * Read a Set as a sequence of ordinals encoded as gap-encoded variable-byte integers.  Use the framework to look up each Object by it's ordinals.
     */
    @Override
    public <T> Set<T> deserializeSet(FastBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<T> itemSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = VarInt.countVarIntsInRange(byteData, fieldPosition, length);

        if(numElements == 0)
            return Collections.emptySet();

        SetBuilder<T> set = minimizedCollections.createSetBuilder();
        set.builderInit(numElements);

        FastBlobTypeDeserializationState<T> elementDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(itemSerializer.getName());

        int previousOrdinal = 0;

        for(int i=0;i<numElements;i++) {
            if(VarInt.readVNull(byteData, fieldPosition)) {
                fieldPosition++;
                set.builderSet(i, null);
            } else {
                int ordinal = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(ordinal);

                ordinal += previousOrdinal;
                previousOrdinal = ordinal;

                T element = elementDeserializationState.get(ordinal);

                if(element != null)
                    set.builderSet(i, element);
            }
        }

        return minimizedCollections.minimizeSet(set.builderFinish());
    }

    /**
     * Read a Map as a sequence of key/value pairs encoded as variable-byte integers (value are gap-encoded).  Use the framework to look up each Object by it's ordinals.
     */
    @Override
    public <K, V> Map<K, V> deserializeMap(FastBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = VarInt.countVarIntsInRange(byteData, fieldPosition, length);

        numElements /= 2;

        if(numElements == 0)
            return Collections.emptyMap();

        MapBuilder<K, V> map = minimizedCollections.createMapBuilder();
        map.builderInit(numElements);

        FastBlobTypeDeserializationState<K> keyDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(keySerializer.getName());
        FastBlobTypeDeserializationState<V> valueDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(valueSerializer.getName());

        populateMap(byteData, fieldPosition, numElements, map, keyDeserializationState, valueDeserializationState);

        return minimizedCollections.minimizeMap(map.builderFinish());
    }

    /**
     * Read a SortedMap as a sequence of key/value pairs encoded as variable-byte integers (value are gap-encoded).  Use the framework to look up each Object by it's ordinals.
     */
    @Override
    public <K, V> SortedMap<K, V> deserializeSortedMap(FastBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if(fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = VarInt.countVarIntsInRange(byteData, fieldPosition, length);

        numElements /= 2;

        if(numElements == 0)
            return minimizedCollections.emptySortedMap();

        MapBuilder<K, V> map = minimizedCollections.createSortedMapBuilder();
        map.builderInit(numElements);

        FastBlobTypeDeserializationState<K> keyDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(keySerializer.getName());
        FastBlobTypeDeserializationState<V> valueDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(valueSerializer.getName());

        populateMap(byteData, fieldPosition, numElements, map, keyDeserializationState, valueDeserializationState);

        return minimizedCollections.minimizeSortedMap( (SortedMap<K, V>) map.builderFinish() );
    }


    private <K, V> void populateMap(ByteData byteData, long fieldPosition, int numElements, MapBuilder<K, V> mapToPopulate, FastBlobTypeDeserializationState<K> keyState, FastBlobTypeDeserializationState<V> valueState) {
        int previousValueOrdinal = 0;

        for(int i=0;i<numElements;i++) {
            K key = null;
            V value = null;

            boolean undefinedKeyOrValue = false;

            if(VarInt.readVNull(byteData, fieldPosition)) {
                fieldPosition++;
            } else {
                int keyOrdinal = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(keyOrdinal);

                key = keyState.get(keyOrdinal);

                if(key == null)
                    undefinedKeyOrValue = true;
            }

            if(VarInt.readVNull(byteData, fieldPosition)) {
                fieldPosition++;
            } else {
                int valueOrdinal = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(valueOrdinal);

                valueOrdinal += previousValueOrdinal;
                previousValueOrdinal = valueOrdinal;

                value = valueState.get(valueOrdinal);

                if(value == null)
                    undefinedKeyOrValue = true;
            }

            if(!undefinedKeyOrValue)
                mapToPopulate.builderPut(i, key, value);
        }
    }

    /**
     * Decode a String as a series of VarInts, one per character.<p/>
     *
     * @param str
     * @param out
     * @return
     * @throws IOException
     */
    private final ThreadLocal<char[]> chararr = new ThreadLocal<char[]>();


    protected String readString(ByteData data, long position, int length) {
        long endPosition = position + length;

        char chararr[] = getCharArray();

        if(length > chararr.length)
            chararr = new char[length];

        int count = 0;

        while(position < endPosition) {
            int c = VarInt.readVInt(data, position);
            chararr[count++] = (char)c;
            position += VarInt.sizeOfVInt(c);
        }

        // The number of chars may be fewer than the number of bytes in the serialized data
        return new String(chararr, 0, count);
    }

    private char[] getCharArray() {
        char ch[] = chararr.get();
        if(ch == null) {
            ch = new char[100];
            chararr.set(ch);
        }
        return ch;
    }

}
