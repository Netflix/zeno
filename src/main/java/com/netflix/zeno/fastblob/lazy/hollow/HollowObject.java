package com.netflix.zeno.fastblob.lazy.hollow;

import static com.netflix.zeno.fastblob.FastBlobFrameworkSerializer.NULL_DOUBLE_BITS;
import static com.netflix.zeno.fastblob.FastBlobFrameworkSerializer.NULL_FLOAT_BITS;

import com.netflix.zeno.fastblob.lazy.LazyStateEngine;
import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.record.schema.MapFieldDefinition;
import com.netflix.zeno.fastblob.record.schema.TypedFieldDefinition;

public class HollowObject {

    private LazyStateEngine lazyStateEngine;
    private FastBlobSchema schema;
    private ByteData data;
    private int ordinal;
    private long position;

    public void position(LazyStateEngine lazyStateEngine, FastBlobSchema schema, ByteData data, int ordinal, long position) {
        this.lazyStateEngine = lazyStateEngine;
        this.schema = schema;
        this.data = data;
        this.ordinal = ordinal;
        this.position = position;
    }

    public int getOrdinal() {
        return ordinal;
    }


    public boolean isNull(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);
        FieldType fieldType = schema.getFieldType(fieldIndex);

        return isNull(fieldType, position);
    }

    public boolean getBoolean(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);
        if(data.get(position) == (byte)1)
            return true;
        else if(data.get(position) == (byte)0)
            return false;
        throw new NullPointerException("Attempting to read a null boolean value");
    }

    public int getInt(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);
        int rawValue = VarInt.readVInt(data, position);
        return (rawValue >>> 1) ^ ((rawValue << 31) >> 31);  // zig-zag encoded.
    }

    public long getLong(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);
        long rawValue = VarInt.readVInt(data, position);
        return (rawValue >>> 1) ^ ((rawValue << 63) >> 63);  // zig-zag encoded.
    }

    public float getFloat(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);
        int intBits = readIntBits(position);

        if(intBits == NULL_FLOAT_BITS)
            throw new NullPointerException("Attempting to read a null float value");

        return Float.intBitsToFloat(intBits);
    }

    public double getDouble(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);
        long longBits = readLongBits(position);

        if(longBits == NULL_DOUBLE_BITS)
            throw new NullPointerException("Attempting to read a null double value");

        return Double.longBitsToDouble(longBits);
    }

    public String getString(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);

        if(VarInt.readVNull(data, position))
            return null;

        int length = VarInt.readVInt(data, position);
        position += VarInt.sizeOfVInt(length);

        return readString(data, position, length);
    }

    public boolean isStringFieldEqual(String fieldName, String testValue) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);

        if(VarInt.readVNull(data, position))
            return testValue == null;
        else if(testValue == null)
            return false;

        int length = VarInt.readVInt(data, position);
        position += VarInt.sizeOfVInt(length);

        return testStringEquality(data, position, length, testValue);
    }

    public HollowObject getObject(String fieldName) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);

        if(VarInt.readVNull(data, position))
            return null;

        TypedFieldDefinition fieldDef = (TypedFieldDefinition) schema.getFieldDefinition(fieldIndex);
        String subType = fieldDef.getSubType();
        int ordinal = VarInt.readVInt(data, position);

        return lazyStateEngine.getHollowObject(subType, ordinal);
    }

    public boolean positionObject(String fieldName, HollowObject objectToPosition) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);

        if(VarInt.readVNull(data, position))
            return false;

        TypedFieldDefinition fieldDef = (TypedFieldDefinition) schema.getFieldDefinition(fieldIndex);
        String subType = fieldDef.getSubType();
        int ordinal = VarInt.readVInt(data, position);

        return lazyStateEngine.positionHollowObject(subType, ordinal, objectToPosition);
    }

    public HollowList getList(String fieldName) {
        HollowList list = new HollowList();

        if(positionCollection(fieldName, list))
            return list;

        return null;
    }

    public HollowSet getSet(String fieldName) {
        HollowSet set = new HollowSet();

        if(positionCollection(fieldName, set))
            return set;

        return null;
    }

    public boolean positionCollection(String fieldName, HollowCollection collectionToPosition) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);

        if(VarInt.readVNull(data, position))
            return false;

        TypedFieldDefinition fieldDef = (TypedFieldDefinition) schema.getFieldDefinition(fieldIndex);

        collectionToPosition.position(lazyStateEngine, fieldDef.getSubType(), data, position);

        return true;
    }

    public HollowMap getMap(String fieldName) {
        HollowMap map = new HollowMap();

        if(positionMap(fieldName, map))
            return map;

        return null;
    }

    public boolean positionMap(String fieldName, HollowMap map) {
        int fieldIndex = schema.getPosition(fieldName);
        long position = positionFor(fieldIndex);

        if(VarInt.readVNull(data, position))
            return false;

        MapFieldDefinition fieldDef = (MapFieldDefinition) schema.getFieldDefinition(fieldIndex);

        map.position(lazyStateEngine, fieldDef.getKeyType(), fieldDef.getValueType(), data, position);

        return true;
    }

    private long positionFor(int fieldIndex) {
        long position = this.position;
        for(int i=0;i<fieldIndex;i++) {
            position += fieldLength(position, schema.getFieldType(i));
        }
        return position;
    }

    private int fieldLength(long currentPosition, FieldType type) {
        if(type.startsWithVarIntEncodedLength()) {
            if(VarInt.readVNull(data, currentPosition)) {
                return 1;
            } else {
                int fieldLength = VarInt.readVInt(data, currentPosition);
                return VarInt.sizeOfVInt(fieldLength) + fieldLength;
            }
        } else if(type.getFixedLength() != -1) {
            return type.getFixedLength();
        } else {
            if(VarInt.readVNull(data, currentPosition)) {
                return 1;
            } else {
                long value = VarInt.readVLong(data, currentPosition);
                return VarInt.sizeOfVLong(value);
            }
        }
    }

    private boolean isNull(FieldType fieldType, long position) {
        switch(fieldType) {
        case FLOAT:
            return readIntBits(position) == NULL_FLOAT_BITS;
        case DOUBLE:
            return readLongBits(position) == NULL_DOUBLE_BITS;
        default:
            return VarInt.readVNull(data, position);
        }
    }

    private int readIntBits(long fieldPosition) {
        int intBits = (data.get(fieldPosition++) & 0xFF) << 24;
        intBits |= (data.get(fieldPosition++) & 0xFF) << 16;
        intBits |= (data.get(fieldPosition++) & 0xFF) << 8;
        intBits |= (data.get(fieldPosition) & 0xFF);
        return intBits;
    }


    private long readLongBits(long fieldPosition) {
        long longBits = (long) (data.get(fieldPosition++) & 0xFF) << 56;
        longBits |= (long) (data.get(fieldPosition++) & 0xFF) << 48;
        longBits |= (long) (data.get(fieldPosition++) & 0xFF) << 40;
        longBits |= (long) (data.get(fieldPosition++) & 0xFF) << 32;
        longBits |= (long) (data.get(fieldPosition++) & 0xFF) << 24;
        longBits |= (data.get(fieldPosition++) & 0xFF) << 16;
        longBits |= (data.get(fieldPosition++) & 0xFF) << 8;
        longBits |= (data.get(fieldPosition) & 0xFF);
        return longBits;
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

    private String readString(ByteData data, long position, int length) {
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

    private boolean testStringEquality(ByteData data, long position, int length, String testValue) {
        if(length < testValue.length()) // can't check exact length here; the length argument is in bytes, which is equal to or greater than the number of characters.
            return false;

        long endPosition = position + length;

        int count = 0;

        while(position < endPosition) {
            int c = VarInt.readVInt(data, position);
            if(testValue.charAt(count++) != (char)c)
                return false;
            position += VarInt.sizeOfVInt(c);
        }

        // The number of chars may be fewer than the number of bytes in the serialized data
        return count == testValue.length();
    }

    private char[] getCharArray() {
        char ch[] = chararr.get();
        if(ch == null) {
            ch = new char[100];
            chararr.set(ch);
        }
        return ch;
    }

    @Override
    public int hashCode() {
        return ordinal;
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof HollowObject) {
            HollowObject hollowObj = (HollowObject)obj;
            return ordinal == hollowObj.getOrdinal();
        }
        return false;
    }

    @Override
    public String toString() {
        return "Hollow Object: " + schema.getName() + " (" + ordinal + ")";
    }

}
