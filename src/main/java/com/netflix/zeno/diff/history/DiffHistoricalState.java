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
import com.netflix.zeno.util.collections.impl.OpenAddressingArraySet;
import com.netflix.zeno.util.collections.impl.OpenAddressingHashMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public Set<String> getTypes() {
        return typeStates.keySet();
    }

    @SuppressWarnings("unchecked")
    public <K, V> DiffHistoricalTypeState<K, V>getTypeState(String objectType) {
        return (DiffHistoricalTypeState<K, V>)typeStates.get(objectType);
    }

    public int numTotalChanges() {
        int totalChanges = 0;
        for(Map.Entry<String, DiffHistoricalTypeState<?, ?>>entry : typeStates.entrySet()) {
            totalChanges += entry.getValue().numChanges();
        }
        return totalChanges;
    }

    public <K, V> void addTypeState(TypeDiffInstruction<?> typeInstruction, Map<K, V> from, Map<K, V> to) {
        String typeIdentifier = typeInstruction.getTypeIdentifier();
        boolean isGroupOfObjects = !typeInstruction.isUniqueKey();

        typeStates.put(typeIdentifier, createTypeState(from, to, isGroupOfObjects));
    }

    /**
     * Create a historical state by determining the differences between the "from" and "to" states for this type.<p/>
     *
     * The key which was chosen for this type may not be unique, in which case both Maps will contain a List of items for each key.
     *
     */
    private <K, V> DiffHistoricalTypeState<K, V> createTypeState(Map<K, V> from, Map<K, V> to, boolean isGroupOfObjects) {
        int newCounter = 0;
        int diffCounter = 0;
        int deleteCounter = 0;

        for(K key : from.keySet()) {
            V toValue = to.get(key);

            if(toValue == null) {
                deleteCounter++;
            } else {
                V fromValue = from.get(key);

                if(!checkEquality(toValue, fromValue, isGroupOfObjects)) {
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
                if(!checkEquality(toValue, fromValue, isGroupOfObjects)) {
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

    /**
     * Equality is different depending on whether or not we are keying by a unique key.<p/>
     *
     * <ul>
     * <li>If the key is unique, then we simply compare equality with ==.</li>
     * <li>If the key is not unique, then we have grouped these elements by the key (in Lists).
     * In this case, we check equality of each element with ==.</li>
     * </ul>
     *
     */
    @SuppressWarnings("unchecked")
    private boolean checkEquality(Object o1, Object o2, boolean isGroupOfObjects) {
        if(isGroupOfObjects) {
            /// equality for a List, in this case, means that for each list, at each element the items are == to one another.
            /// we know that the element ordering is the same because we iterated over the objects in ordinal order from the type
            /// state when we built the list in the DiffHistoryDataState
            List<Object> l1 = (List<Object>)o1;
            List<Object> l2 = (List<Object>)o2;

            if(l1.size() != l2.size())
                return false;

            for(int i=0;i<l1.size();i++) {
                if(l1.get(i) != l2.get(i))
                    return false;
            }

            return true;
        } else {
            return o1 == o2;
        }
    }

}
