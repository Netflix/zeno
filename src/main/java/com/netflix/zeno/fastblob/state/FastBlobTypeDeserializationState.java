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

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.util.CollectionUnwrapper;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * This class represents the "deserialization state" for a single type at some level of the object
 * hierarchy in the FastBlob serialized data.<p/>
 *
 * This class is responsible for maintaining the mappings between ordinals and deserialized objects.
 * It performs this responsibility by maintaining an ArrayList of objects.  The location of the object
 * in the ArrayList will be the index of its ordinal.
 *
 * @param <T>
 *
 * @author dkoszewnik
 *
 */
public class FastBlobTypeDeserializationState<T> implements Iterable<T> {

    private final NFTypeSerializer<T> serializer;

    private TypeDeserializationStateListener<T> stateListener = TypeDeserializationStateListener.noopCallback();

    private List<T> objects;

    ///the following properties are used for heap-friendly double snapshot refresh
    private List<T> previousObjects;
    private BitSet copiedPreviousObjects;
    private ObjectIdentityOrdinalMap identityOrdinalMap;

    public FastBlobTypeDeserializationState(NFTypeSerializer<T> serializer) {
        this.serializer = serializer;
        this.objects = new ArrayList<T>();
    }

    public T get(int ordinal) {
        if(ordinal >= objects.size())
            return null;
        return objects.get(ordinal);
    }

    @SuppressWarnings("deprecation")
    public void add(int ordinal, FastBlobDeserializationRecord rec) {
        T obj = serializer.deserialize(rec);
        ensureCapacity(ordinal + 1);
        objects.set(ordinal, obj);
        stateListener.addedObject(obj);
        stateListener.addedObject(obj, ordinal);
    }

    @SuppressWarnings("deprecation")
    public void remove(int ordinal) {
        T removedObject = objects.get(ordinal);
        objects.set(ordinal, null);
        stateListener.removedObject(removedObject);
        stateListener.removedObject(removedObject, ordinal);
    }

    public void setListener(TypeDeserializationStateListener<T> listener) {
        this.stateListener = listener;
    }

    /**
     * Not intended for external consumption.<p/>
     *
     * This method is only intended to be used during heap-friendly double snapshot refresh.
     */
    public void populateByteArrayOrdinalMap(ByteArrayOrdinalMap ordinalMap) {
        FastBlobSerializationRecord rec = new FastBlobSerializationRecord(serializer.getFastBlobSchema());
        ByteDataBuffer scratch = new ByteDataBuffer();

        for(int i=0;i<objects.size();i++) {
            T obj = objects.get(i);
            if(obj != null) {
                serializer.serialize(obj, rec);
                rec.writeDataTo(scratch);
                ordinalMap.put(scratch, i);
                scratch.reset();
                rec.reset();
            }
        }

        previousObjects = objects;
        copiedPreviousObjects = new BitSet(previousObjects.size());
        objects = new ArrayList<T>(previousObjects.size());
    }

    /**
     * Fill this state from the serialized data which exists in this ByteArrayOrdinalMap
     *
     * @param ordinalMap
     */
    public void populateFromByteOrdinalMap(final ByteArrayOrdinalMap ordinalMap) {
        ByteDataBuffer byteData = ordinalMap.getByteData();
        AtomicLongArray pointersAndOrdinals = ordinalMap.getPointersAndOrdinals();
        FastBlobDeserializationRecord rec = new FastBlobDeserializationRecord(getSchema(), byteData.getUnderlyingArray());
        for (int i = 0; i < pointersAndOrdinals.length(); i++) {
            long pointerAndOrdinal = pointersAndOrdinals.get(i);
            if(!ByteArrayOrdinalMap.isPointerAndOrdinalEmpty(pointerAndOrdinal)) {
                long pointer = ByteArrayOrdinalMap.getPointer(pointerAndOrdinal);
                int ordinal = ByteArrayOrdinalMap.getOrdinal(pointerAndOrdinal);

                int sizeOfData = VarInt.readVInt(byteData.getUnderlyingArray(), pointer);
                pointer += VarInt.sizeOfVInt(sizeOfData);

                rec.position(pointer);

                add(ordinal, rec);
            }
        }
    }

    /**
     * Not intended for external consumption.<p/>
     *
     * This method is only intended to be used during heap-friendly double snapshot refresh.
     */
    public void createIdentityOrdinalMap() {
        identityOrdinalMap = new ObjectIdentityOrdinalMap(objects);
    }

    /**
     * Not intended for external consumption.<p/>
     *
     * This method is only intended to be used during heap-friendly double snapshot refresh.
     */
    public int find(T obj) {
        if(identityOrdinalMap == null)
            return -1;

        int ordinal = identityOrdinalMap.get(obj);

        if(ordinal < 0)
            ordinal = identityOrdinalMap.get(CollectionUnwrapper.unwrap(obj));

        return ordinal;
    }

    /**
     * Not intended for external consumption.<p/>
     *
     * This method is only intended to be used during heap-friendly double snapshot refresh.
     */
    public void copyPrevious(int newOrdinal, int previousOrdinal) {
        T obj = previousObjects.get(previousOrdinal);
        ensureCapacity(newOrdinal + 1);
        objects.set(newOrdinal, obj);
        copiedPreviousObjects.set(previousOrdinal);
        stateListener.reassignedObject(obj, previousOrdinal, newOrdinal);
    }

    /**
     * Not intended for external consumption.<p/>
     *
     * This method is only intended to be used during heap-friendly double snapshot refresh.
     */
    @SuppressWarnings("deprecation")
    public void clearPreviousObjects() {
        /// each previous object which was *not* copied was removed
        for(int i=0;i<previousObjects.size();i++) {
            T t = previousObjects.get(i);
            if(t != null && !copiedPreviousObjects.get(i)) {
                stateListener.removedObject(t);
                stateListener.removedObject(t, i);
            }
        }
        previousObjects = null;
        copiedPreviousObjects = null;
    }

    /**
     * Not intended for external consumption.<p/>
     *
     * This method is only intended to be used during heap-friendly double snapshot refresh.
     */
    public void clearIdentityOrdinalMap() {
        identityOrdinalMap = null;
    }

    public FastBlobSchema getSchema() {
        return serializer.getFastBlobSchema();
    }

    public NFTypeSerializer<T> getSerializer() {
        return serializer;
    }

    /**
     * Counts the number of populated objects in this state.<p/>
     *
     * @return an integer equal to the number of objects which will be iterated over by the Iterator
     * returned from iterator();
     */
    public int countObjects() {
        int count = 0;
        for(int i=0;i<objects.size();i++) {
            if(objects.get(i) != null)
                count++;
        }
        return count;
    }

    /**
     * Returns the current maximum ordinal for this type.  Returns -1 if this type has no objects.
     *
     * @return
     */
    public int maxOrdinal() {
        int ordinal = objects.size();
        while(--ordinal >= 0) {
            if(objects.get(ordinal) != null)
                return ordinal;
        }
        return -1;
    }

    @Override
    public Iterator<T> iterator() {
        return new TypeDeserializationStateIterator<T>(objects);
    }

    void ensureCapacity(int size) {
        while(objects.size() < size) {
            objects.add(null);
        }
    }

    public void fillSerializationState(FastBlobStateEngine engine) {
        for (T t : this) {
            engine.add(serializer.getName(), t);
        }
    }

}
