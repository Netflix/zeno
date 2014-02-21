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

import static com.netflix.zeno.flatblob.FlatBlobFrameworkSerializer.NULL_DOUBLE_BITS;
import static com.netflix.zeno.flatblob.FlatBlobFrameworkSerializer.NULL_FLOAT_BITS;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.FrameworkDeserializer;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.util.collections.CollectionImplementation;
import com.netflix.zeno.util.collections.MinimizedUnmodifiableCollections;
import com.netflix.zeno.util.collections.builder.ListBuilder;
import com.netflix.zeno.util.collections.builder.MapBuilder;
import com.netflix.zeno.util.collections.builder.SetBuilder;

public class FlatBlobFrameworkDeserializer extends FrameworkDeserializer<FlatBlobDeserializationRecord>{

    private final Map<String, FlatBlobTypeCache<?>> typeCaches;
    private final ThreadLocal<Map<String, FlatBlobDeserializationRecord>> deserializationRecords;

    private MinimizedUnmodifiableCollections minimizedCollections = new MinimizedUnmodifiableCollections(CollectionImplementation.JAVA_UTIL);

    public void setCollectionImplementation(CollectionImplementation impl) {
        minimizedCollections = new MinimizedUnmodifiableCollections(impl);
    }

    protected FlatBlobFrameworkDeserializer(FlatBlobSerializationFramework framework) {
        super(framework);
        this.typeCaches = new HashMap<String, FlatBlobTypeCache<?>>();
        this.deserializationRecords = new ThreadLocal<Map<String,FlatBlobDeserializationRecord>>();

        for(NFTypeSerializer<?> serializer : framework.getOrderedSerializers()) {
            typeCaches.put(serializer.getName(), new FlatBlobTypeCache<Object>(serializer.getName()));
        }
    }

    @Override
    public Boolean deserializeBoolean(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public boolean deserializePrimitiveBoolean(FlatBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        return byteData.get(fieldPosition) == (byte) 1;
    }

    @Override
    public Integer deserializeInteger(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public int deserializePrimitiveInt(FlatBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        int value = VarInt.readVInt(byteData, fieldPosition);

        return (value >>> 1) ^ ((value << 31) >> 31);
    }

    @Override
    public Long deserializeLong(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public long deserializePrimitiveLong(FlatBlobDeserializationRecord rec, String fieldName) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        long value = VarInt.readVLong(byteData, fieldPosition);

        return (value >>> 1) ^ ((value << 63) >> 63);
    }

    /**
     * Read a float as a fixed-length sequence of 4 bytes.  Might be null.
     */
    @Override
    public Float deserializeFloat(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public float deserializePrimitiveFloat(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public Double deserializeDouble(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public double deserializePrimitiveDouble(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public String deserializeString(FlatBlobDeserializationRecord rec, String fieldName) {
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
    public byte[] deserializeBytes(FlatBlobDeserializationRecord rec, String fieldName) {
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


    @Override
    public <T> T deserializeObject(FlatBlobDeserializationRecord rec, String fieldName, Class<T> clazz) {
        long position = rec.getPosition(fieldName);
        if (position == -1)
            return null;
        return deserializeObject(rec, position, rec.getObjectType(fieldName));
    }

    /**
     * @deprecated use instead deserializeObject(FlatBlobDeserializationRecord rec, String fieldName, Class<T> clazz);
     */
    @Deprecated
    @Override
    public <T> T deserializeObject(FlatBlobDeserializationRecord rec, String fieldName, String typeName, Class<T> clazz) {
        long position = rec.getPosition(fieldName);
        if (position == -1)
            return null;
        return deserializeObject(rec, position, typeName);
    }

    @SuppressWarnings("unchecked")
    private <T> T deserializeObject(FlatBlobDeserializationRecord rec, long position, String typeName) {
        ByteData underlyingData = rec.getByteData();

        if (position == -1 || VarInt.readVNull(underlyingData, position))
            return null;

        int ordinal = VarInt.readVInt(underlyingData, position);

        FlatBlobTypeCache<T> typeCache = getTypeCache(typeName);
        T cached = typeCache.get(ordinal);
        if(cached != null)
            return cached;

        position += VarInt.sizeOfVInt(ordinal);

        int sizeOfUnderlyingData = VarInt.readVInt(underlyingData, position);

        position += VarInt.sizeOfVInt(sizeOfUnderlyingData);

        FlatBlobDeserializationRecord subRec = getDeserializationRecord(typeName);
        subRec.setByteData(rec.getByteData());
        subRec.setCacheElements(rec.shouldCacheElements());
        subRec.position(position);

        T deserialized = (T) framework.getSerializer(typeName).deserialize(subRec);

        if(rec.shouldCacheElements()) {
            deserialized = typeCache.putIfAbsent(ordinal, deserialized);
        }

        return deserialized;
    }

    @Override
    public <T> List<T> deserializeList(FlatBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<T> itemSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = countFlatBlobElementsInRange(byteData, fieldPosition, length);

        if(numElements == 0)
            return Collections.emptyList();

        FlatBlobTypeCache<T>typeCache = getTypeCache(itemSerializer.getName());

        ListBuilder<T> listBuilder = minimizedCollections.createListBuilder();
        listBuilder.builderInit(numElements);

        for(int i=0;i<numElements;i++) {
            if(VarInt.readVNull(byteData, fieldPosition)) {
                listBuilder.builderSet(i, null);
                fieldPosition += 1;
            } else {
                int ordinal = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(ordinal);
                int sizeOfData = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(sizeOfData);

                T cached = typeCache.get(ordinal);
                if(cached != null) {
                    listBuilder.builderSet(i, cached);
                } else {
                    FlatBlobDeserializationRecord elementRec = getDeserializationRecord(itemSerializer.getName());
                    elementRec.setByteData(rec.getByteData());
                    elementRec.setCacheElements(rec.shouldCacheElements());
                    elementRec.position(fieldPosition);
                    T deserialized = itemSerializer.deserialize(elementRec);

                    if(rec.shouldCacheElements()) {
                        deserialized = typeCache.putIfAbsent(ordinal, deserialized);
                    }

                    listBuilder.builderSet(i, deserialized);
                }

                fieldPosition += sizeOfData;
            }
        }

        return listBuilder.builderFinish();
    }

    @Override
    public <T> Set<T> deserializeSet(FlatBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<T> itemSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = countFlatBlobSetElementsInRange(byteData, fieldPosition, length);

        if(numElements == 0)
            return Collections.emptySet();

        FlatBlobTypeCache<T>typeCache = getTypeCache(itemSerializer.getName());

        SetBuilder<T> setBuilder = minimizedCollections.createSetBuilder();
        setBuilder.builderInit(numElements);

        int previousOrdinal = 0;

        for(int i=0;i<numElements;i++) {
            if(VarInt.readVNull(byteData, fieldPosition) && VarInt.readVNull(byteData, fieldPosition + 1)) {
                setBuilder.builderSet(i, null);
                fieldPosition += 1;
            } else {
                int ordinal = -1;
                if(VarInt.readVNull(byteData, fieldPosition)) {
                    fieldPosition++;
                } else {
                    ordinal = VarInt.readVInt(byteData, fieldPosition);
                    fieldPosition += VarInt.sizeOfVInt(ordinal);
                    ordinal += previousOrdinal;
                    previousOrdinal = ordinal;
                }
                int sizeOfData = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(sizeOfData);

                T cached = typeCache.get(ordinal);
                if(cached != null) {
                    setBuilder.builderSet(ordinal, cached);
                    fieldPosition += sizeOfData;
                    continue;
                }

                FlatBlobDeserializationRecord elementRec = getDeserializationRecord(itemSerializer.getName());
                elementRec.setByteData(rec.getByteData());
                elementRec.setCacheElements(rec.shouldCacheElements());
                elementRec.position(fieldPosition);
                T deserialized = itemSerializer.deserialize(elementRec);

                if(rec.shouldCacheElements()) {
                    deserialized = typeCache.putIfAbsent(ordinal, deserialized);
                }

                setBuilder.builderSet(i, deserialized);
                fieldPosition += sizeOfData;
            }
        }

        return setBuilder.builderFinish();
    }

    @Override
    public <K, V> Map<K, V> deserializeMap(FlatBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if (fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = countFlatBlobElementsInRange(byteData, fieldPosition, length);

        numElements /= 2;

        if(numElements == 0)
            return Collections.emptyMap();

        MapBuilder<K, V> map = minimizedCollections.createMapBuilder();
        map.builderInit(numElements);

        FlatBlobTypeCache<K> keyCache = getTypeCache(keySerializer.getName());
        FlatBlobTypeCache<V> valueCache = getTypeCache(valueSerializer.getName());

        populateMap(byteData, fieldPosition, numElements, map, keySerializer, keyCache, valueSerializer, valueCache, rec.shouldCacheElements());

        return minimizedCollections.minimizeMap(map.builderFinish());
    }

    @Override
    public <K, V> SortedMap<K, V> deserializeSortedMap(FlatBlobDeserializationRecord rec, String fieldName, NFTypeSerializer<K> keySerializer, NFTypeSerializer<V> valueSerializer) {
        ByteData byteData = rec.getByteData();
        long fieldPosition = rec.getPosition(fieldName);

        if(fieldPosition == -1 || VarInt.readVNull(byteData, fieldPosition))
            return null;

        int length = VarInt.readVInt(byteData, fieldPosition);
        fieldPosition += VarInt.sizeOfVInt(length);

        int numElements = countFlatBlobElementsInRange(byteData, fieldPosition, length);

        numElements /= 2;

        if(numElements == 0)
            return minimizedCollections.emptySortedMap();

        MapBuilder<K, V> map = minimizedCollections.createSortedMapBuilder();
        map.builderInit(numElements);

        FlatBlobTypeCache<K> keyCache = getTypeCache(keySerializer.getName());
        FlatBlobTypeCache<V> valueCache = getTypeCache(valueSerializer.getName());

        populateMap(byteData, fieldPosition, numElements, map, keySerializer, keyCache, valueSerializer, valueCache, rec.shouldCacheElements());

        return minimizedCollections.minimizeSortedMap( (SortedMap<K, V>) map.builderFinish() );
    }

    private <K, V> void populateMap(ByteData byteData, long fieldPosition, int numElements, MapBuilder<K, V> mapToPopulate, NFTypeSerializer<K> keySerializer, FlatBlobTypeCache<K> keyCache, NFTypeSerializer<V> valueSerializer, FlatBlobTypeCache<V> valueCache, boolean shouldCacheElements) {
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
                int sizeOfData = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(sizeOfData);

                key = keyCache.get(keyOrdinal);
                if(key == null) {
                    FlatBlobDeserializationRecord rec = getDeserializationRecord(keyCache.getName());
                    rec.setByteData(byteData);
                    rec.setCacheElements(shouldCacheElements);
                    rec.position(fieldPosition);
                    key = keySerializer.deserialize(rec);
                    if(shouldCacheElements)
                        key = keyCache.putIfAbsent(keyOrdinal, key);
                }

                fieldPosition += sizeOfData;

                if(key == null)
                    undefinedKeyOrValue = true;
            }

            if(VarInt.readVNull(byteData, fieldPosition)) {
                fieldPosition++;
            } else {
                int valueOrdinal = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(valueOrdinal);
                int sizeOfData = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(sizeOfData);

                valueOrdinal += previousValueOrdinal;
                previousValueOrdinal = valueOrdinal;

                value = valueCache.get(valueOrdinal);
                if(value == null) {
                    FlatBlobDeserializationRecord rec = getDeserializationRecord(valueCache.getName());
                    rec.setByteData(byteData);
                    rec.setCacheElements(shouldCacheElements);
                    rec.position(fieldPosition);
                    value = valueSerializer.deserialize(rec);
                    if(shouldCacheElements)
                        value = valueCache.putIfAbsent(valueOrdinal, value);
                }

                fieldPosition += sizeOfData;

                if(value == null)
                    undefinedKeyOrValue = true;
            }

            if(!undefinedKeyOrValue)
                mapToPopulate.builderPut(i, key, value);
        }
    }

    private int countFlatBlobSetElementsInRange(ByteData byteData, long fieldPosition, int length) {
        int numElements = 0;
        long endPosition = length + fieldPosition;

        while(fieldPosition < endPosition) {
            if(VarInt.readVNull(byteData, fieldPosition) && VarInt.readVNull(byteData, fieldPosition + 1)) {
                fieldPosition += 2;
            } else {
                if(VarInt.readVNull(byteData, fieldPosition)) {
                    fieldPosition += 1;
                } else {
                    int ordinal = VarInt.readVInt(byteData, fieldPosition);
                    fieldPosition += VarInt.sizeOfVInt(ordinal);
                }

                int eLen = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(eLen);
                fieldPosition += eLen;
            }
            numElements++;
        }

        return numElements;
    }

    private int countFlatBlobElementsInRange(ByteData byteData, long fieldPosition, int length) {
        int numElements = 0;
        long endPosition = length + fieldPosition;

        while(fieldPosition < endPosition) {
            if(VarInt.readVNull(byteData, fieldPosition)) {
                fieldPosition += 1;
            } else {
                int ordinal = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(ordinal);
                int eLen = VarInt.readVInt(byteData, fieldPosition);
                fieldPosition += VarInt.sizeOfVInt(eLen);
                fieldPosition += eLen;
            }
            numElements++;
        }

        return numElements;
    }

    /**
     * Decode a String as a series of VarInts, one per character.<p/>
     *
     */
    private final ThreadLocal<char[]> chararr = new ThreadLocal<char[]>();

    private String readString(ByteData data, long position, int length) {
        long endPosition = position + length;

        char chararr[] = getCharArray(length);

        int count = 0;

        while(position < endPosition) {
            int c = VarInt.readVInt(data, position);
            chararr[count++] = (char)c;
            position += VarInt.sizeOfVInt(c);
        }

        // The number of chars may be fewer than the number of bytes in the serialized data
        return new String(chararr, 0, count);
    }

    private char[] getCharArray(int length) {
        if(length < 100)
            length = 100;

        char ch[] = chararr.get();
        if(ch == null || ch.length < length) {
            ch = new char[length];
            chararr.set(ch);
        }

        return ch;
    }

    FlatBlobDeserializationRecord getDeserializationRecord(String type) {
        Map<String, FlatBlobDeserializationRecord> map = deserializationRecords.get();
        if(map == null) {
            map = new HashMap<String, FlatBlobDeserializationRecord>();
            deserializationRecords.set(map);
        }

        FlatBlobDeserializationRecord rec = map.get(type);

        if(rec == null) {
            FastBlobSchema schema = framework.getSerializer(type).getFastBlobSchema();
            rec = new FlatBlobDeserializationRecord(schema);
            map.put(type, rec);
        }

        return rec;
    }

    @SuppressWarnings("unchecked")
    <T> FlatBlobTypeCache<T> getTypeCache(String type) {
        return (FlatBlobTypeCache<T>)typeCaches.get(type);
    }

}
