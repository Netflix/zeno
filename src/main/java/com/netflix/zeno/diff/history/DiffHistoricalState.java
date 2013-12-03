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

import com.netflix.zeno.util.collections.impl.OpenAddressingArraySet;
import com.netflix.zeno.util.collections.impl.OpenAddressingHashMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a historical set of changes in a version of a FastBlobStateEngine
 *
 * @author dkoszewnik
 *
 */
public class DiffHistoricalState {

    private final String version;
    private final Map<String, DiffHistoricalTypeState<?, ?>> typeStates;

    public DiffHistoricalState(String version) {
        this.version = version;
        this.typeStates = new ConcurrentHashMap<String, DiffHistoricalTypeState<?, ?>>();
    }

    public String getVersion() {
        return version;
    }

    @SuppressWarnings("unchecked")
    public <K, V> DiffHistoricalTypeState<K, V>getMap(String objectType) {
        return (DiffHistoricalTypeState<K, V>)typeStates.get(objectType);
    }

    public <K, V> void addTypeState(String typeName, Map<K, V> from, Map<K, V> to) {
        typeStates.put(typeName, createTypeState(from, to));
    }


    private <K, V> DiffHistoricalTypeState<K, V> createTypeState(Map<K, V> from, Map<K, V> to) {
        int newCounter = 0;
        int diffCounter = 0;
        int deleteCounter = 0;

        for(K key : from.keySet()) {
            V toValue = to.get(key);

            if(toValue == null) {
                deleteCounter++;
            } else {
                V fromValue = from.get(key);

                if(fromValue != toValue) {
                    diffCounter++;
                }
            }
        }

        for(K key : to.keySet()) {
            if(!from.containsKey(key)) {
                newCounter++;
            }
        }

        OpenAddressingArraySet<K> newSet = new OpenAddressingArraySet<K>();
        OpenAddressingHashMap<K, V> diffMap = new OpenAddressingHashMap<K, V>();
        OpenAddressingHashMap<K, V> deleteMap = new OpenAddressingHashMap<K, V>();

        newSet.builderInit(newCounter);
        diffMap.builderInit(diffCounter);
        deleteMap.builderInit(deleteCounter);

        newCounter = diffCounter = deleteCounter = 0;

        for(K key : from.keySet()) {
            V fromValue = from.get(key);
            V toValue = to.get(key);

            if(toValue == null) {
                deleteMap.builderPut(deleteCounter++, key, fromValue);
            } else {
                if(fromValue != toValue) {
                    diffMap.builderPut(diffCounter++, key, fromValue);
                }
            }
        }

        for(K key : to.keySet()) {
            if(!from.containsKey(key)) {
                newSet.builderSet(newCounter++, key);
            }
        }

        newSet.builderFinish();
        diffMap.builderFinish();
        deleteMap.builderFinish();

        return new DiffHistoricalTypeState<K, V>(newSet, diffMap, deleteMap);
    }

}
