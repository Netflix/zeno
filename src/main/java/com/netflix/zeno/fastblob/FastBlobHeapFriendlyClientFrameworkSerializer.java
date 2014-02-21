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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema.FieldType;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;

/**
 * Rather than adding objects to a serialization state and having the ByteArrayOrdinalMap assign ordinals,
 * during a double snapshot refresh, we must determine the ordinal of an object based on its deserialization state's
 * == mapping. <p/>
 *
 * This class overrides the appropriate methods to inject this functionality.
 *
 * @author dkoszewnik
 *
 */
public class FastBlobHeapFriendlyClientFrameworkSerializer extends FastBlobFrameworkSerializer {

    private boolean checkSerializationIntegrity = false;
    private boolean serializationIntegrityFlawed = false;

    public FastBlobHeapFriendlyClientFrameworkSerializer(FastBlobStateEngine framework) {
        super(framework);
    }

    @Override
    protected void serializeObject(FastBlobSerializationRecord rec, int position, String fieldName, String typeName, Object obj) {
        if(obj == null)
            return;

        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.OBJECT)
            throw new IllegalArgumentException("Attempting to serialize an Object as " + fieldType + " in field " + fieldName + ".  Carefully check your schema for type " + rec.getSchema().getName() + ".");

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        FastBlobTypeDeserializationState<Object> deserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(typeName);

        int ordinal = findObject(deserializationState, obj, fieldName);

        VarInt.writeVInt(fieldBuffer, ordinal);
    }

    @Override
    public <T> void serializeList(FastBlobSerializationRecord rec, String fieldName, String typeName, Collection<T> collection) {
        if(collection == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.LIST && fieldType != FieldType.COLLECTION)
            throw new IllegalArgumentException("Attempting to serialize a List as " + fieldType + " in field " + fieldName);

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);

        FastBlobTypeDeserializationState<Object> deserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(typeName);

        for (T obj : collection) {
            if(obj == null) {
                VarInt.writeVNull(fieldBuffer);
            } else {
                int ordinal = findObject(deserializationState, obj, fieldName);
                VarInt.writeVInt(fieldBuffer, ordinal);
            }
        }
    }

    @Override
    public <T> void serializeSet(FastBlobSerializationRecord rec, String fieldName, String typeName, Set<T> set) {
        if(set == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.SET && fieldType != FieldType.COLLECTION)
            throw new IllegalArgumentException("Attempting to serialize a Set as " + fieldType + " in field " + fieldName);

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);
        FastBlobTypeDeserializationState<Object> deserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(typeName);
        int setOrdinals[] = new int[set.size()];

        int i = 0;
        for (T obj : set) {
            setOrdinals[i++] = findObject(deserializationState, obj, fieldName);
        }

        Arrays.sort(setOrdinals);

        int currentOrdinal = 0;

        for (i = 0; i < setOrdinals.length; i++) {
            VarInt.writeVInt(fieldBuffer, setOrdinals[i] - currentOrdinal);
            currentOrdinal = setOrdinals[i];
        }
    }

    @Override
    public <K, V> void serializeMap(FastBlobSerializationRecord rec, String fieldName, String keyTypeName, String valueTypeName, Map<K, V> map) {
        if(map == null)
            return;

        int position = rec.getSchema().getPosition(fieldName);
        FieldType fieldType = rec.getSchema().getFieldType(position);

        if(fieldType != FieldType.MAP)
            throw new IllegalArgumentException("Attempting to serialize a Map as " + fieldType + " in field " + fieldName);

        ByteDataBuffer fieldBuffer = rec.getFieldBuffer(position);
        FastBlobTypeDeserializationState<Object> keyDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(keyTypeName);
        FastBlobTypeDeserializationState<Object> valueDeserializationState = ((FastBlobStateEngine) framework).getTypeDeserializationState(valueTypeName);

        long mapEntries[] = new long[map.size()];

        int i = 0;
        for (Map.Entry<K, V> entry : map.entrySet()) {
            int keyOrdinal = -1;
            int valueOrdinal = -1;
            if(entry.getKey() != null)
                keyOrdinal = findObject(keyDeserializationState, entry.getKey(), fieldName + "(key)");
            if(entry.getValue() != null)
                valueOrdinal = findObject(valueDeserializationState, entry.getValue(), fieldName + "(value)");


            mapEntries[i++] = ((long)valueOrdinal << 32) | (keyOrdinal & 0xFFFFFFFFL);
        }

        Arrays.sort(mapEntries);

        int currentValueOrdinal = 0;

        for (i = 0; i < mapEntries.length; i++) {
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
            }

            currentValueOrdinal = valueOrdinal;
        }
    }

    private <T> int findObject(FastBlobTypeDeserializationState<Object> deserializationState, T obj, String fieldName) {
        int ordinal = deserializationState.find(obj);

        if(checkSerializationIntegrity && ordinal < 0) {
            serializationIntegrityFlawed = true;
        }

        return ordinal;
    }

    public void setCheckSerializationIntegrity(boolean warn) {
        this.checkSerializationIntegrity = warn;
    }

    public boolean isSerializationIntegrityFlawed() {
        return serializationIntegrityFlawed;
    }

    public void clearSerializationIntegrityFlawedFlag() {
        serializationIntegrityFlawed = false;
    }

}
