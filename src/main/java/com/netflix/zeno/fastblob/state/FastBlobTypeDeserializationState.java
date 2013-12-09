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

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.FastBlobSchema;
import com.netflix.zeno.fastblob.record.FastBlobSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.util.CollectionUnwrapper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

    private List<T> objects;

    ///the following properties are used for heap-friendly double snapshot refresh
    private List<T> previousObjects;
    private ObjectIdentityOrdinalMap identityOrdinalMap;

    public FastBlobTypeDeserializationState(NFTypeSerializer<T> serializer) {
        this.serializer = serializer;
        this.objects = new ArrayList<T>();
    }

    public T get(int ordinal) {
        return objects.get(ordinal);
    }

    public void add(int ordinal, FastBlobDeserializationRecord rec) {
        T obj = serializer.deserialize(rec);
        ensureCapacity(ordinal + 1);
        objects.set(ordinal, obj);
    }

    public void remove(int ordinal) {
        objects.set(ordinal, null);
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
        objects = new ArrayList<T>(previousObjects.size());
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
    }

    /**
     * Not intended for external consumption.<p/>
     *
     * This method is only intended to be used during heap-friendly double snapshot refresh.
     */
    public void clearPreviousObjects() {
        previousObjects = null;
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

    @Override
    public Iterator<T> iterator() {
        return new TypeDeserializationStateIterator<T>(objects);
    }

    private void ensureCapacity(int size) {
        while(objects.size() < size) {
            objects.add(null);
        }
    }
}
