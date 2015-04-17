/*
 *
 *  Copyright 2015 Netflix, Inc.
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
package com.netflix.zeno.util.collections.heapfriendly;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link Map} that wraps a {@link HeapFriendlyHashMap}
 * and exposes methods for switching in and out of a data swap phase. <p />
 * 
 * The data swap phase is entered by calling {@link PhasedHeapFriendlyHashMap#beginDataSwapPhase(int)} <p />
 * 
 * While in the data swap phase, {@link PhasedHeapFriendlyHashMap#put(Object, Object)}
 * can be used to fill the map with data. Data will not be available until the data swap phase is complete. <p />
 * 
 * Calling {@link PhasedHeapFriendlyHashMap#endDataSwapPhase()} will end the
 * data swap phase and make the data added available
 *
 * @author drbathgate
 *
 * @param <K> Key
 * @param <V> Value
 */
public class PhasedHeapFriendlyHashMap<K, V> implements Map<K, V> {

    private final HeapFriendlyMapArrayRecycler recycler;

    private HeapFriendlyHashMap<K, V> currentMap;
    private HeapFriendlyHashMap<K, V> nextMap;

    public PhasedHeapFriendlyHashMap() {
        this.recycler = new HeapFriendlyMapArrayRecycler();
        this.currentMap = new HeapFriendlyHashMap<K, V>(0, recycler);
    }

    @Override
    public int size() {
        return currentMap.size();
    }

    @Override
    public boolean isEmpty() {
        return currentMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return currentMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return currentMap.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return currentMap.get(key);
    }

    @Override
    public V put(K key, V value) {
        if (nextMap != null) {
            return nextMap.put(key, value);
        }

        throw new IllegalStateException("PhasedHeapFriendlyHashMap.put(K, V) only usable when in the data swap phase");
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException("PhasedHeapFriendlyHashMap.remove(Object) not supported");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new UnsupportedOperationException("PhasedHeapFriendlyHashMap.putAll(Map) not supported, please use PhasedHeapFriendlyHashMap.put(Object) instead");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("PhasedHeapFriendlyHashMap.clear() not supported");
    }

    @Override
    public Set<K> keySet() {
        return currentMap.keySet();
    }

    @Override
    public Collection<V> values() {
        return currentMap.values();
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return currentMap.entrySet();
    }

    /**
     * Enters data swap phase<p />
     *
     * While in data swap phase, {@link PhasedHeapFriendlyHashMap#put(Object, Object)} can be used<p />
     *
     * @param numOfNewEntries Number of new entries expected to be added during the the data swap phase
     */
    public void beginDataSwapPhase(int numOfNewEntries){

        if (nextMap != null) {
            throw new IllegalStateException("Cannot call PhasedHeapFriendlyHashMap.beginDataSwapPhase(int), already in data swap phase");
        }

        recycler.swapCycleObjectArrays();

        nextMap = new HeapFriendlyHashMap<K, V>(numOfNewEntries, recycler);
    }

    /**
     * Ends the data swap phase<p />
     *
     * While out of the data swap phase, using {@link PhasedHeapFriendlyHashMap#put(Object, Object)}
     * will throw an {@link IllegalStateException} <p />
     */
    public void endDataSwapPhase(){

        if (nextMap == null) {
            throw new IllegalStateException("Cannot call PhasedHeapFriendlyHashMap.endDataSwapPhase(), not currently in data swap phase");
        }

        HeapFriendlyHashMap<K, V> temp = currentMap;

        currentMap = nextMap;
        nextMap = null;

        temp.releaseObjectArrays();

        recycler.clearNextCycleObjectArrays();
    }
}