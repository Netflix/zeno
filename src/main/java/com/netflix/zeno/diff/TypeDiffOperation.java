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

import com.netflix.zeno.diff.TypeDiff.FieldDiffScore;
import com.netflix.zeno.serializer.NFTypeSerializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
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
        return performDiff(framework, fromState, toState, Runtime.getRuntime().availableProcessors());
    }

    @SuppressWarnings("unchecked")
    public TypeDiff<T> performDiff(DiffSerializationFramework framework, Iterable<T> fromState, Iterable<T> toState, int numThreads) {
        Map<Object, T> fromStateObjects = new HashMap<Object, T>();

        for(T obj : fromState) {
            fromStateObjects.put(instruction.getKey(obj), obj);
        }

        ArrayList<List<T>> perProcessorWorkList = new ArrayList<List<T>>(numThreads); // each entry is a job
        for (int i =0; i < numThreads; ++i) {
            perProcessorWorkList.add(new ArrayList<T>());
        }

        Map<Object, Object> toStateKeys = new ConcurrentHashMap<Object, Object>();

        int toIncrCount = 0;
        for(T toObject : toState) {
            perProcessorWorkList.get(toIncrCount % numThreads).add(toObject);
            toIncrCount++;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                final Thread thread = new Thread(r, "TypeDiff_" + instruction.getTypeIdentifier());
                thread.setDaemon(true);
                return thread;
            }
        });

        try {
            ArrayList<Future<TypeDiff<T>>> workResultList = new ArrayList<Future<TypeDiff<T>>>(perProcessorWorkList.size());
            for (final List<T> workList : perProcessorWorkList) {
                if (workList != null && !workList.isEmpty()) {
                    workResultList.add(executor.submit(new TypeDiffCallable<T>(framework, instruction, fromStateObjects, toStateKeys, workList)));
                }
            }

            TypeDiff<T> mergedDiff = new TypeDiff<T>(instruction.getTypeIdentifier());
            for (final Future<TypeDiff<T>> future : workResultList) {
                try {
                    TypeDiff<T> typeDiff = future.get();
                    mergeTypeDiff(mergedDiff, typeDiff);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            for(Map.Entry<Object, T> entry : fromStateObjects.entrySet()) {
                mergedDiff.incrementFrom();
                if(!toStateKeys.containsKey(entry.getKey()))
                    mergedDiff.addExtraInFrom(entry.getValue());
            }

            return mergedDiff;

        } finally {
            executor.shutdownNow();
        }
    }

    private void mergeTypeDiff(TypeDiff<T> mergedDiff, TypeDiff<T> typeDiff) {

        mergedDiff.getExtraInFrom().addAll(typeDiff.getExtraInFrom());
        mergedDiff.getExtraInTo().addAll(typeDiff.getExtraInTo());
        mergedDiff.getDiffObjects().addAll(typeDiff.getDiffObjects());
        mergedDiff.incrementFrom(typeDiff.getItemCountFrom());
        mergedDiff.incrementTo(typeDiff.getItemCountTo());

        Map<DiffPropertyPath, FieldDiffScore<T>> mergedFieldDifferences = mergedDiff.getFieldDifferences();
        Map<DiffPropertyPath, FieldDiffScore<T>> fieldDifferences = typeDiff.getFieldDifferences();
        for (final DiffPropertyPath path : fieldDifferences.keySet()) {
            FieldDiffScore<T> fieldDiffScore = fieldDifferences.get(path);

            FieldDiffScore<T> mergedFieldDiffScore = mergedFieldDifferences.get(path);
            if (mergedFieldDiffScore != null) {
                mergedFieldDiffScore.incrementDiffCountBy(fieldDiffScore.getDiffCount());
                mergedFieldDiffScore.incrementTotalCountBy(fieldDiffScore.getTotalCount());
                mergedFieldDiffScore.getDiffScores().addAll(fieldDiffScore.getDiffScores());
            } else {
                mergedFieldDifferences.put(path, fieldDiffScore);
            }
        }
    }

    class TypeDiffCallable<Z> implements Callable<TypeDiff<Z>> {

        private final TypeDiffInstruction<Z> instruction;
        private final List<Z> workList;
        private final Map<Object, Object> toStateKeys;
        private final Map<Object, Z> fromStateObjects;
        private final DiffSerializationFramework framework;

        public TypeDiffCallable(DiffSerializationFramework framework, TypeDiffInstruction<Z> instruction, Map<Object, Z> fromStateObjects, Map<Object, Object> toStateKeys, List<Z> workList) {
            this.framework = framework;
            this.instruction = instruction;
            this.fromStateObjects = fromStateObjects;
            this.toStateKeys = toStateKeys;
            this.workList = workList;
        }

        @Override
        public TypeDiff<Z> call() throws Exception {
            TypeDiff<Z> diff = new TypeDiff<Z>(instruction.getTypeIdentifier());
            NFTypeSerializer<Z> typeSerializer = (NFTypeSerializer<Z>) framework.getSerializer(instruction.getSerializerName());

            DiffRecord fromRec = new DiffRecord();
            fromRec.setSchema(typeSerializer.getFastBlobSchema());
            DiffRecord toRec = new DiffRecord();
            toRec.setSchema(typeSerializer.getFastBlobSchema());
            fromRec.setTopLevelSerializerName(instruction.getSerializerName());
            toRec.setTopLevelSerializerName(instruction.getSerializerName());

            for(Z toObject : workList) {
                diff.incrementTo();
                Object toStateKey = instruction.getKey(toObject);
                toStateKeys.put(toStateKey, Boolean.TRUE);
                Z fromObject = fromStateObjects.get(toStateKey);

                if(fromObject == null) {
                    diff.addExtraInTo(toObject);
                } else {
                    int diffScore = diffFields(diff, fromRec, toRec, typeSerializer, toObject, fromObject);
                    if(diffScore > 0)
                        diff.addDiffObject(fromObject, toObject, diffScore);
                }
            }

            return diff;
        }

        private int diffFields(TypeDiff<Z> diff, DiffRecord fromRec, DiffRecord toRec, NFTypeSerializer<Z> typeSerializer, Z toObject, Z fromObject) {
            typeSerializer.serialize(toObject, toRec);
            typeSerializer.serialize(fromObject, fromRec);

            int diffScore = incrementDiffFields(diff, toRec, fromRec, toObject, fromObject);

            toRec.clear();
            fromRec.clear();

            return diffScore;
        }

        private int incrementDiffFields(TypeDiff<Z> diff, DiffRecord toRecord, DiffRecord fromRecord, Z toObject, Z fromObject) {
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
