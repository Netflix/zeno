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
package com.netflix.zeno.examples;

import com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyHashMap;
import com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyMapArrayRecycler;

import java.util.Map;

import org.junit.Test;

/**
 * Example usage of a "heap-friendly" hash map.  For details, follow 
 * along in the Zeno documentation on the page <a href="https://github.com/Netflix/zeno/wiki/Making-data-available">making data available</a>
 * 
 * @author dkoszewnik
 *
 */
public class HeapFriendlyHashMapExample {

    public HeapFriendlyHashMap<Integer, String> accessMap;


    @Test
    public void runCycle() {
        runCycle(1, 2, 3, 4, 5, 6, 7, 8);
        runCycle(1, 2, 3, 4, 5, 6, 7, 9);
        runCycle(1, 2, 3, 4, 5, 6, 7, 10);
        runCycle(1, 2, 3, 4, 5, 6, 7, 11);
        runCycle(1, 2, 3, 4, 5, 6, 7, 12);

        for(Map.Entry<Integer, String> entry : accessMap.entrySet()) {
            System.out.println(entry.getKey() + ":\"" + entry.getValue() + "\"");
        }
    }

    /**
     * For each cycle, we need to perform some administrative tasks.
     */
    public void runCycle(int... valuesForMap) {
        /// prepare the map Object array recycler for a new cycle.
        HeapFriendlyMapArrayRecycler.get().swapCycleObjectArrays();

        try {
            makeDataAvailableToApplication(valuesForMap);
        } finally {
            // fill all of the Object arrays which were returned to the pool on this cycle with nulls,
            // so that the garbage collector can clean up any data which is no longer used.
            HeapFriendlyMapArrayRecycler.get().clearNextCycleObjectArrays();
        }
    }

    public void makeDataAvailableToApplication(int... valuesForMap) {
        HeapFriendlyHashMap<Integer, String> newAccessMap = new HeapFriendlyHashMap<Integer, String>(valuesForMap.length);

        for(int val : valuesForMap) {
            Integer key = Integer.valueOf(val);
            String value = String.valueOf(val);

            newAccessMap.put(key, value);
        }

        /// release the object arrays for the current access map
        if(accessMap != null)
            accessMap.releaseObjectArrays();

        /// make the new access map available
        accessMap = newAccessMap;
    }



}
