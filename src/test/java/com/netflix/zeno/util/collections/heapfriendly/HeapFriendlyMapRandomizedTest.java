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
package com.netflix.zeno.util.collections.heapfriendly;

import com.netflix.zeno.util.collections.heapfriendly.AbstractHeapFriendlyMap;
import com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyDerivableKeyHashMap;
import com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyHashMap;
import com.netflix.zeno.util.collections.heapfriendly.HeapFriendlyMapArrayRecycler;

import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HeapFriendlyMapRandomizedTest {

    private final Random rand = new Random();

    @Before
    public void setUp() {
        HeapFriendlyMapArrayRecycler.get().clearNextCycleObjectArrays();
    }


    @Test
    public void randomlyTestHeapFriendlyMap() {

        /// 4 cycles
        for(int i=0;i<4;i++) {
            ///prepare by swapping the cycle Object arrays
            HeapFriendlyMapArrayRecycler.get().swapCycleObjectArrays();

            AbstractHeapFriendlyMap<Integer, String> map = createRandomMap(10000);

            /// arrays may be "released" as long as the clearNextCycleObjectArrays is not called before calls are completed.
            map.releaseObjectArrays();

            int counter = 0;

            for(Map.Entry<Integer, String> entry : map.entrySet()) {
               int key = entry.getKey().intValue();
               String value = entry.getValue();

               /// the key should be mapped to the expected value
               Assert.assertEquals(key, Integer.parseInt(value));
               /// and we should be able to look up that value
               Assert.assertEquals(value, map.get(entry.getKey()));

               counter++;
            }

            /// make sure map.size() is correct
            Assert.assertEquals(counter, map.size());

            /// must clear all arrays in the recycler before they are reused in the next cycle.
            HeapFriendlyMapArrayRecycler.get().clearNextCycleObjectArrays();
        }
    }



    private AbstractHeapFriendlyMap<Integer, String> createRandomMap(int numElements) {
        if(rand.nextBoolean()) {
            HeapFriendlyHashMap<Integer, String> map = new HeapFriendlyHashMap<Integer, String>(numElements);

            for(int i=0;i<numElements;i++) {
                int value = rand.nextInt();

                map.put(Integer.valueOf(value), String.valueOf(value));
            }

            return map;
        } else {
            HeapFriendlyDerivableKeyHashMap<Integer, String> map = new HeapFriendlyDerivableKeyHashMap<Integer, String>(numElements) {
                protected Integer deriveKey(String value) {
                    return Integer.valueOf(value);
                }
            };

            for(int i=0;i<numElements;i++) {
                int value = rand.nextInt();

                map.put(String.valueOf(value));
            }

            return map;
        }
    }
}
