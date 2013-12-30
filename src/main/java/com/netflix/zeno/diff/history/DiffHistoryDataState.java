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
package com.netflix.zeno.diff.history;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;

import com.netflix.zeno.diff.TypeDiffInstruction;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.util.collections.impl.OpenAddressingHashMap;

/**
 * A complete historical representation of the objects available in a
 * FastBlobStateEngine at some point in time.<p/>
 * 
 * This representation contains all of the objects of each type specified in a
 * set of TypeDiffInstructions, keyed by the keys specified in thos
 * TypeDiffInstructions.
 * 
 * @author dkoszewnik
 * 
 */
public class DiffHistoryDataState {

    private final String version;
    private final TypeDiffInstruction<?>[] typeInstructions;
    private final Map<String, Map<?, ?>> typeStates;

    /**
     * Create a new DiffHistoryDataState. Pulls data from the supplied
     * FastBlobStateEngine in the manner specified by the supplied set of
     * TypeDiffInstructions.
     * 
     * @param stateEngine
     * @param typeInstructions
     */
    @SuppressWarnings("unchecked")
    public DiffHistoryDataState(FastBlobStateEngine stateEngine, TypeDiffInstruction<?>... typeInstructions) {
        this.version = stateEngine.getLatestVersion();
        this.typeInstructions = typeInstructions;
        this.typeStates = new HashMap<String, Map<?, ?>>();

        for (TypeDiffInstruction<?> instruction : typeInstructions) {
            FastBlobTypeDeserializationState<Object> typeState = stateEngine.getTypeDeserializationState(instruction.getSerializerName());
            addTypeState(typeState, (TypeDiffInstruction<Object>) instruction);
        }
    }

    private <T> void addTypeState(FastBlobTypeDeserializationState<T> deserializationState, TypeDiffInstruction<T> instruction) {
        if (instruction.isUniqueKey())
            buildUniqueKeyTypeState(deserializationState, instruction);
        else
            buildGroupedTypeState(deserializationState, instruction);
    }

    private <T> void buildUniqueKeyTypeState(FastBlobTypeDeserializationState<T> deserializationState, TypeDiffInstruction<T> instruction) {
        OpenAddressingHashMap<Object, T> typeState = new OpenAddressingHashMap<Object, T>();
        typeState.builderInit(deserializationState.countObjects());

        int counter = 0;

        for (T obj : deserializationState) {
            Object key = instruction.getKeyFromObject(obj);

            typeState.builderPut(counter++, key, obj);
        }

        typeState.builderFinish();

        typeStates.put(instruction.getTypeIdentifier(), typeState);
    }

    private <T> void buildGroupedTypeState(FastBlobTypeDeserializationState<T> deserializationState, TypeDiffInstruction<T> instruction) {
        Map<Object, MutableInt> countsByKey = countObjectsByKey(deserializationState, instruction);

        Map<Object, List<T>> groupsByKey = groupObjectsByKey(deserializationState, instruction, countsByKey);

        OpenAddressingHashMap<Object, List<T>> typeState = buildNewTypeState(groupsByKey);

        typeStates.put(instruction.getTypeIdentifier(), typeState);
    }

    private <T> OpenAddressingHashMap<Object, List<T>> buildNewTypeState(Map<Object, List<T>> groupsByKey) {
        OpenAddressingHashMap<Object, List<T>> typeState = new OpenAddressingHashMap<Object, List<T>>();
        typeState.builderInit(groupsByKey.size());

        int counter = 0;

        for (Map.Entry<Object, List<T>> entry : groupsByKey.entrySet()) {
            typeState.builderPut(counter++, entry.getKey(), entry.getValue());
        }

        typeState.builderFinish();
        return typeState;
    }

    private <T> Map<Object, List<T>> groupObjectsByKey(FastBlobTypeDeserializationState<T> deserializationState, TypeDiffInstruction<T> instruction, Map<Object, MutableInt> countsByKey) {
        Map<Object, List<T>> groupsByKey = new HashMap<Object, List<T>>(countsByKey.size());

        for (T obj : deserializationState) {
            Object key = instruction.getKeyFromObject(obj);
            List<T> groupList = groupsByKey.get(key);
            if (groupList == null) {
                int count = countsByKey.get(key).intValue();
                groupList = new ArrayList<T>(count);
                groupsByKey.put(key, groupList);
            }

            groupList.add(obj);
        }
        return groupsByKey;
    }

    private <T> Map<Object, MutableInt> countObjectsByKey(FastBlobTypeDeserializationState<T> deserializationState, TypeDiffInstruction<T> instruction) {
        Map<Object, MutableInt> countsByKey = new HashMap<Object, MutableInt>(deserializationState.countObjects());

        for (T obj : deserializationState) {
            Object key = instruction.getKeyFromObject(obj);

            MutableInt count = (MutableInt) countsByKey.get(key);
            if (count == null) {
                count = new MutableInt(0);
                countsByKey.put(key, count);
            }

            count.increment();
        }
        return countsByKey;
    }

    public TypeDiffInstruction<?>[] getTypeDiffInstructions() {
        return typeInstructions;
    }

    public String getVersion() {
        return version;
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getTypeState(String name) {
        return (Map<K, V>) typeStates.get(name);
    }

}
