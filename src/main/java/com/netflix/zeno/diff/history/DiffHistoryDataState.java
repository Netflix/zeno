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

import com.netflix.zeno.diff.TypeDiffInstruction;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.util.collections.impl.OpenAddressingHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A complete historical representation of the objects available in a FastBlobStateEngine at some point in time.
 *
 * This representation contains all of the objects of each type specified in a set of TypeDiffInstructions, keyed
 * by the keys specified in thos TypeDiffInstructions.
 *
 * @author dkoszewnik
 *
 */
public class DiffHistoryDataState {

    private final String version;
    private final Map<String, Map<?, ?>> typeStates;

    /**
     * Create a new DiffHistoryDataState.  Pulls data from the supplied FastBlobStateEngine in the manner
     * specified by the supplied set of TypeDiffInstructions.
     *
     * @param stateEngine
     * @param typeInstructions
     */
    @SuppressWarnings("unchecked")
    public DiffHistoryDataState(FastBlobStateEngine stateEngine, TypeDiffInstruction<?> typeInstructions[]) {
        this.version = stateEngine.getLatestVersion();
        this.typeStates = new HashMap<String, Map<?, ?>>();

        for(TypeDiffInstruction<?> instruction : typeInstructions) {
            FastBlobTypeDeserializationState<Object> typeState = stateEngine.getTypeDeserializationState(instruction.getSerializerName());
            addTypeState(typeState, (TypeDiffInstruction<Object>)instruction);
        }
    }

    private <T> void addTypeState(FastBlobTypeDeserializationState<T> deserializationState, TypeDiffInstruction<T> instruction) {
        OpenAddressingHashMap<Object, T> typeState = new OpenAddressingHashMap<Object, T>();
        typeState.builderInit(deserializationState.countObjects());

        int counter = 0;

        for(T obj : deserializationState) {
            Object key = instruction.getKeyFromObject(obj);

            typeState.builderPut(counter++, key, obj);
        }

        typeState.builderFinish();

        typeStates.put(instruction.getSerializerName(), typeState);
    }

    public Set<String> getTypeStateNames() {
        return typeStates.keySet();
    }

    public String getVersion() {
        return version;
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getTypeState(String name) {
        return (Map<K, V>)typeStates.get(name);
    }

}
