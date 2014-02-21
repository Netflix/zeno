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
package com.netflix.zeno.fastblob.record.schema;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * A schema for a record contained in a FastBlob.<p/>
 *
 * The fields each have a position, which is the order in which they will appear in a FastBlob serialized representation.<p/>
 *
 * The schema is a hash table of Strings (field name) to field position.<p/>
 *
 * Schemas are flat lists of fields, each specified by (fieldName, fieldType, objectType). objectType will be null for primitive types.
 *
 * @author dkoszewnik
 *
 */
public class FastBlobSchema {

    private final String schemaName;

    private final int hashedPositionArray[];
    private final String fieldNames[];
    private final FieldDefinition fieldDefinitions[];

    private int size;

    public FastBlobSchema(String schemaName, int numFields) {
        this.schemaName = schemaName;

        this.hashedPositionArray = new int[1 << (32 - Integer.numberOfLeadingZeros(numFields * 10 / 7))];
        this.fieldNames = new String[numFields];
        this.fieldDefinitions = new FieldDefinition[numFields];

        Arrays.fill(hashedPositionArray, -1);
    }

    public String getName() {
        return schemaName;
    }

    /**
     * Add a field into this <code>FastBlobSchema</code>.
     *
     * @return the position of the field.
     *
     * @deprecated use addField with a new FieldDefinition instead.
     */
    @Deprecated
    public int addField(String fieldName, FieldType fieldType) {
        return addField(fieldName, new FieldDefinition(fieldType));
    }

    /**
     * Add a field into this <code>FastBlobSchema</code>.
     *
     * The position of the field is hashed into the <code>hashedPositionArray</code> by the hashCode of the fieldName.<p/>
     *
     * Create a new FieldDefinition for your type as follows:<p/>
     *
     * <ul>
     * <li>For a primitive value, use new FieldDefinition(...)</li>
     * <li>For an OBJECT, LIST, or SET, use new TypedFieldDefinition(...)</li>
     * <li>For a MAP, use new MapFieldDefinition(...)</li>
     * </ul>
     *
     * @return the position of the field.
     */
    public int addField(String fieldName, FieldDefinition fieldDefinition) {
        fieldNames[size] = fieldName;
        fieldDefinitions[size] = fieldDefinition;
        hashPositionIntoArray(size);

        return size++;
    }

    /**
     * Returns the position of a field previously added to the map, or -1 if the field has not been added to the map.
     *
     * The positions of the fields are hashed into the <code>hashedPositionArray</code> by the hashCode of the fieldName.
     */
    public int getPosition(String fieldName) {
        int hash = hashInt(fieldName.hashCode());

        int bucket = hash % hashedPositionArray.length;
        int position = hashedPositionArray[bucket];

        while(position != -1) {
            if(fieldNames[position].equals(fieldName))
                return position;

            bucket = (bucket + 1) % hashedPositionArray.length;
            position = hashedPositionArray[bucket];
        }

        return -1;
    }

    /**
     * @return The name of the field at the specified position
     */
    public String getFieldName(int fieldPosition) {
        return fieldNames[fieldPosition];
    }

    /**
     * @return The type of the field with the given name
     */
    public FieldType getFieldType(String fieldName) {
        int position = getPosition(fieldName);

        if(position == -1)
            throw new IllegalArgumentException("Field name " + fieldName + " does not exist in schema " + schemaName);

        return fieldDefinitions[position].getFieldType();
    }

    /**
     * @return The type of the field at the specified position
     */
    public FieldType getFieldType(int fieldPosition) {
        return fieldDefinitions[fieldPosition].getFieldType();
    }

    /**
     * @return The type of the field with the given name
     */
    public FieldDefinition getFieldDefinition(String fieldName) {
        int position = getPosition(fieldName);

        if(position == -1)
            throw new IllegalArgumentException("Field name " + fieldName + " does not exist in schema " + schemaName);

        return getFieldDefinition(position);
    }

    /**
     * @return The FieldDefinition at the specified position
     */
    public FieldDefinition getFieldDefinition(int fieldPosition) {
        return fieldDefinitions[fieldPosition];
    }

    /**
     * @return the object type of the field with the given name
     */
    public String getObjectType(String fieldName) {
        int position = getPosition(fieldName);

        if(position == -1)
            throw new IllegalArgumentException("Field name " + fieldName + " does not exist in schema " + schemaName);

        return getObjectType(position);
    }

    /**
     * @return The object type at the specified position
     */
    public String getObjectType(int fieldPosition) {
        if(fieldDefinitions[fieldPosition] instanceof TypedFieldDefinition) {
            return ((TypedFieldDefinition)fieldDefinitions[fieldPosition]).getSubType();
        }

        return null;
    }

    /**
     * @return The number of fields in this schema.
     */
    public int numFields() {
        return size;
    }

    private void hashPositionIntoArray(int ordinal) {
        int hash = hashInt(fieldNames[ordinal].hashCode());

        int bucket = hash % hashedPositionArray.length;

        while(hashedPositionArray[bucket] != -1) {
            bucket = (bucket + 1) % hashedPositionArray.length;
        }

        hashedPositionArray[bucket] = ordinal;
    }

    private int hashInt(int key) {
        key = ~key + (key << 15);
        key = key ^ (key >>> 12);
        key = key + (key << 2);
        key = key ^ (key >>> 4);
        key = key * 2057;
        key = key ^ (key >>> 16);
        return key & Integer.MAX_VALUE;
    }

    @Override
    public boolean equals(Object other) {
        if(other instanceof FastBlobSchema) {
            FastBlobSchema otherSchema = (FastBlobSchema) other;
            if(otherSchema.schemaName.equals(schemaName)) {
                if(otherSchema.size == size) {
                    for(int i=0;i<otherSchema.size;i++) {
                        if(!otherSchema.getFieldName(i).equals(getFieldName(i))) {
                            return false;
                        }

                        if(!otherSchema.getFieldDefinition(i).equals(getFieldDefinition(i))) {
                            return false;
                        }
                    }

                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Write this FastBlobSchema to a stream.
     */
    public void writeTo(DataOutputStream dos) throws IOException {
        dos.writeUTF(schemaName);
        dos.writeShort(size);
        for(int i=0;i<size;i++) {
            dos.writeUTF(fieldNames[i]);
            writeFieldDefinition(dos, fieldDefinitions[i]);
        }
    }

    private void writeFieldDefinition(DataOutputStream dos, FieldDefinition def)throws IOException {
        FieldType fieldType = def.getFieldType();
        dos.writeUTF(fieldType.name());

        if(fieldType == FieldType.OBJECT || fieldType == FieldType.LIST || fieldType == FieldType.SET) {
            if(def instanceof TypedFieldDefinition) {
                dos.writeUTF(((TypedFieldDefinition)def).getSubType());
            } else {
                dos.writeUTF("");
            }
        } else if(fieldType == FieldType.MAP) {
            if(def instanceof MapFieldDefinition) {
                MapFieldDefinition mfd = (MapFieldDefinition)def;
                dos.writeUTF(mfd.getKeyType());
                dos.writeUTF(mfd.getValueType());
            } else {
                dos.writeUTF("");
                dos.writeUTF("");
            }
        }
    }

    /**
     * Read a FastBlobSchema from a stream.
     */
    public static FastBlobSchema readFrom(DataInputStream dis) throws IOException {
        String name = dis.readUTF();
        int size = dis.readShort();

        FastBlobSchema schema = new FastBlobSchema(name, size);

        for(int i=0;i<size;i++) {
            String fieldName = dis.readUTF();
            FieldDefinition def = readFieldDefinition(dis);

            schema.addField(fieldName, def);
        }

        return schema;
    }

    private static FieldDefinition readFieldDefinition(DataInputStream dis) throws IOException {
        FieldType fieldType = Enum.valueOf(FieldType.class, dis.readUTF());

        if(fieldType == FieldType.OBJECT || fieldType == FieldType.LIST || fieldType == FieldType.SET) {
            String subType = dis.readUTF();

            if(!subType.isEmpty())
                return new TypedFieldDefinition(fieldType, subType);
        } else if(fieldType == FieldType.MAP) {
            String keyType = dis.readUTF();
            String valueType = dis.readUTF();

            if(!keyType.isEmpty())
                return new MapFieldDefinition(keyType, valueType);
        }

        return new FieldDefinition(fieldType);
    }


    /**
     * All allowable field types.
     */
    public static enum FieldType {
        OBJECT(-1, false),
        BOOLEAN(1, false),
        INT(-1, false),
        LONG(-1, false),
        FLOAT(4, false),
        DOUBLE(8, false),
        STRING(-1, true),
        BYTES(-1, true),
        LIST(-1, true),
        SET(-1, true),
        @Deprecated
        /**
         * @deprecated Use SET or LIST instead of COLLECTION
         */
        COLLECTION(-1, true),
        MAP(-1, true);

        private final int fixedLength;
        private final boolean varIntEncodesLength;

        private FieldType(int fixedLength, boolean varIntEncodesLength) {
            this.fixedLength = fixedLength;
            this.varIntEncodesLength = varIntEncodesLength;
        }

        public int getFixedLength() {
            return fixedLength;
        }

        public boolean startsWithVarIntEncodedLength() {
            return varIntEncodesLength;
        }
    }

}

