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

import com.netflix.zeno.util.collections.heapfriendly.PhasedHeapFriendlyHashMap;

import java.util.Map;

import org.junit.Test;

/**
 * Example usage of {@link PhasedHeapFriendlyHashMap}
 * 
 * @author drbathgate
 *
 */
public class PhasedHeapFriendlyHashMapExample {

    @Test
    public void runExample() {
        PhasedHeapFriendlyHashMap<Integer, String> map = new PhasedHeapFriendlyHashMap<Integer, String>();

        fillMap(map, 1, 2, 3, 4, 5, 6, 7, 8);
        printMapEntries(map);

        fillMap(map, 1, 2, 3, 4, 5, 6, 7, 9);
        printMapEntries(map);

        fillMap(map, 1, 2, 3, 4, 5, 6, 7, 10);
        printMapEntries(map);

        fillMap(map, 1, 2, 3, 4, 5, 6, 7, 11);
        printMapEntries(map);

    }

    private void fillMap(PhasedHeapFriendlyHashMap<Integer, String> map, int... entries) {

        // allow access to put data by starting data swap phase
        map.beginDataSwapPhase(entries.length);

        for(int entry: entries) {

            // entries will not be available until the end of the data swap phase
            map.put(entry, String.valueOf(entry));
        }

        // make data available by ending data swap phase
        map.endDataSwapPhase();
    }

    private void printMapEntries(PhasedHeapFriendlyHashMap<Integer, String> map) {
        for(Map.Entry<Integer, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ":\"" + entry.getValue() + "\"");
        }
    }
}
