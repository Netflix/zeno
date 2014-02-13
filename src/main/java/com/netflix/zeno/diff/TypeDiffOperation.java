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
package com.netflix.zeno.diff;

import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;

/**
 * This is the main interface for performing a diff between two arbitrary data states.<p/>
 * 
 * @author dkoszewnik
 *
 */
public class TypeDiffOperation<T> {

    private final TypeDiffInstruction<T> instruction;

    public TypeDiffOperation(TypeDiffInstruction<T> instruction) {
        this.instruction = instruction;
    }

    @SuppressWarnings("unchecked")
    public TypeDiff<T> performDiff(DiffSerializationFramework framework, Iterable<T> fromState, Iterable<T> toState) {
        TypeDiff<T> diff = new TypeDiff<T>(instruction.getTypeIdentifier());
        NFTypeSerializer<T> typeSerializer = (NFTypeSerializer<T>) framework.getSerializer(instruction.getSerializerName());
        
        DiffRecord fromRec = new DiffRecord(typeSerializer.getFastBlobSchema());
        DiffRecord toRec = new DiffRecord(typeSerializer.getFastBlobSchema());
        fromRec.setTopLevelSerializerName(instruction.getSerializerName());
        toRec.setTopLevelSerializerName(instruction.getSerializerName());

        Map<Object, T> fromStateObjects = new HashMap<Object, T>();

        for(T obj : fromState) {
            diff.incrementFrom();
            fromStateObjects.put(instruction.getKey(obj), obj);
        }

        Set<Object> toStateKeys = new HashSet<Object>();

        for(T toObject : toState) {
            diff.incrementTo();
            Object toStateKey = instruction.getKey(toObject);
            toStateKeys.add(toStateKey);
            T fromObject = fromStateObjects.get(toStateKey);

            if(fromObject == null) {
                diff.addExtraInTo(toObject);
            } else {
                int diffScore = diffFields(diff, fromRec, toRec, typeSerializer, toObject, fromObject);
                if(diffScore > 0)
                    diff.addDiffObject(fromObject, toObject, diffScore);
            }
        }

        for(Map.Entry<Object, T> entry : fromStateObjects.entrySet()) {
            if(!toStateKeys.contains(entry.getKey()))
                diff.addExtraInFrom(entry.getValue());
        }

        return diff;
    }

    private int diffFields(TypeDiff<T> diff, DiffRecord fromRec, DiffRecord toRec, NFTypeSerializer<T> typeSerializer, T toObject, T fromObject) {
        typeSerializer.doSerialize(toObject, toRec);
        typeSerializer.doSerialize(fromObject, fromRec);

        int diffScore = incrementDiffFields(diff, toRec, fromRec, toObject, fromObject);

        toRec.clear();
        fromRec.clear();

        return diffScore;
    }

    private int incrementDiffFields(TypeDiff<T> diff, DiffRecord toRecord, DiffRecord fromRecord, T toObject, T fromObject) {
        int objectDiffScore = 0;

        for(DiffPropertyPath key : toRecord.getFieldValues().keySet()) {
            List<Object> toObjects = toRecord.getFieldValues().getList(key);
            List<Object> fromObjects = fromRecord.getFieldValues().getList(key);
            int objectFieldDiffScore;

            if(fromObjects == null) {
                diff.incrementFieldScores(key, toObjects.size(), toObjects.size());
                objectFieldDiffScore = toObjects.size();
            } else {
                objectFieldDiffScore = incrementDiffFields(diff, key, toObjects, fromObjects);
            }

            objectDiffScore += objectFieldDiffScore;

            diff.addFieldObjectDiffScore(key, toObject, fromObject, objectFieldDiffScore);
        }

        for(DiffPropertyPath key : fromRecord.getFieldValues().keySet()) {
            if(toRecord.getFieldValues().getList(key) == null) {
                int diffSize = fromRecord.getFieldValues().getList(key).size();
                diff.incrementFieldScores(key, diffSize, diffSize);
                objectDiffScore += diffSize;

                diff.addFieldObjectDiffScore(key, toObject, fromObject, diffSize);
            }
        }

        return objectDiffScore;
    }

    private int incrementDiffFields(TypeDiff<?> diff, DiffPropertyPath breadcrumbs, List<Object> toObjects, List<Object> fromObjects) {
        int objectFieldDiffScore = 0;

        Map<Object, MutableInt> objectSet = getObjectMap();

        for(Object obj : toObjects) {
            increment(objectSet, obj);
        }

        for(Object obj : fromObjects) {
            if(!decrement(objectSet, obj)) {
                objectFieldDiffScore++;
            }
        }

        if(!objectSet.isEmpty()) {
            for(Map.Entry<Object, MutableInt>entry : objectSet.entrySet()) {
                objectFieldDiffScore += entry.getValue().intValue();
            }
        }

        objectSet.clear();

        diff.incrementFieldScores(breadcrumbs, objectFieldDiffScore, toObjects.size() + fromObjects.size());
        return objectFieldDiffScore;
    }

    private void increment(Map<Object, MutableInt> map, Object obj) {
        MutableInt i = map.get(obj);
        if(i == null) {
            i = new MutableInt(0);
            map.put(obj, i);
        }
        i.increment();
    }

    private boolean decrement(Map<Object, MutableInt> map, Object obj) {
        MutableInt i = map.get(obj);
        if(i == null) {
            return false;
        }

        i.decrement();

        if(i.intValue() == 0) {
            map.remove(obj);
        }

        return true;
    }

    private static final ThreadLocal<Map<Object, MutableInt>> objectSet = new ThreadLocal<Map<Object, MutableInt>>();

    private Map<Object, MutableInt> getObjectMap() {
        Map<Object, MutableInt> objectSet = TypeDiffOperation.objectSet.get();
        if(objectSet == null) {
            objectSet = new HashMap<Object, MutableInt>();
            TypeDiffOperation.objectSet.set(objectSet);
        }
        return objectSet;
    }


}
