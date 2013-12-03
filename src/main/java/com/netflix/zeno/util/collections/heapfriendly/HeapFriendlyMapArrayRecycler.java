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

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Contains two pools of Object[] arrays.<p/>
 *
 * When Netflix's Video Metadata Service receives a FastBlob delta update, it applies the delta to it's FastBlobStateEngine, then indexes
 * many Objects by their primary keys in hash tables.<p/>
 *
 * Because ParNew is a stop the world event, and  ParNew time is directly proportional to the number of Objects which survive after
 * creation and must be copied to survivor spaces / OldGen, we can reduce the GC impact of Map creation if we reuse Objects which
 * have already been promoted to OldGen.<p/>
 *
 * We maintain two pools of Object[] arrays.  One is for the "current" cycle, and one is for the "next" cycle.
 * On each cycle, we take the Object arrays comprising the current HeapFriendlyHashMap segmented arrays, and
 * return them to the "next" cycle pool.  When we create new HeapFriendlyHashMap objects, we construct the
 * segmented arrays with segments from the "current" cycle pool.<p/>
 *
 * At the beginning of each update cycle, we swap the pointers to the "current" and "next" cycle pools.  This way, we're always
 * overwriting the data from 2 cycles ago, and the Object arrays just remain in OldGen.
 *
 * @author dkoszewnik
 *
 */
public class HeapFriendlyMapArrayRecycler {

    public static final int INDIVIDUAL_OBJECT_ARRAY_SIZE = 4096;

    private LinkedList<Object[]> currentCycleObjects;
    private LinkedList<Object[]> nextCycleObjects;

    public HeapFriendlyMapArrayRecycler() {
        this.currentCycleObjects = new LinkedList<Object[]>();
        this.nextCycleObjects = new LinkedList<Object[]>();
    }

    public Object[] getObjectArray() {
        if(!currentCycleObjects.isEmpty()) {
            return currentCycleObjects.removeFirst();
        }

        return new Object[INDIVIDUAL_OBJECT_ARRAY_SIZE];
    }

    public void returnObjectArray(Object[] toReturn) {
        nextCycleObjects.addLast(toReturn);
    }

    public void clearNextCycleObjectArrays() {
        for(Object[] arr : nextCycleObjects) {
            Arrays.fill(arr, null);
        }
    }

    public void swapCycleObjectArrays() {
        LinkedList<Object[]> temp = currentCycleObjects;
        currentCycleObjects = nextCycleObjects;
        nextCycleObjects = temp;
    }

    public void clear() {
        currentCycleObjects.clear();
        nextCycleObjects.clear();
    }

    private final static HeapFriendlyMapArrayRecycler theInstance = new HeapFriendlyMapArrayRecycler();

    public static HeapFriendlyMapArrayRecycler get() {
        return theInstance;
    }

}
