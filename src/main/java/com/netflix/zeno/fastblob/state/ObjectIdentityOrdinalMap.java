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
package com.netflix.zeno.fastblob.state;

import java.util.Arrays;
import java.util.List;

/**
 * A mapping of objects to their ordinals.  For the purposes of this mapping, objects are only found if they are == to each other.<p/>
 *
 * This is used during "heap-friendly" double snapshot refreshes with the FastBlob.<p/>
 *
 * The vast majority of the extra memory required to maintain this mapping is the hashedOrdinals[] array, which is just an int array.  The values of this
 * array are the ordinals of the objects located at the position of each object's identity hash.  Collisions are resolved via linear probing.
 *
 * @author dkoszewnik
 *
 */
public class ObjectIdentityOrdinalMap {

    private final List<Object> objects;
    private final int hashedOrdinals[];
    private final int hashModMask;

    /**
     * The List of Objects passed in here should be the same list as held by the FastBlobTypeDeserializationState.<p/>
     *
     * These Objects should be arranged in ordinal order, that is, the Object with ordinal x is contained at index x in the List.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ObjectIdentityOrdinalMap(List objects) {
        int size = 0;

        for(int i=0;i<objects.size();i++) {
            if(objects.get(i) != null)
                size++;
        }

        int arraySize = (size * 10) / 8; // 80% load factor

        arraySize = 1 << (32 - Integer.numberOfLeadingZeros(arraySize));

        hashedOrdinals = new int[arraySize];
        hashModMask = arraySize - 1;

        Arrays.fill(hashedOrdinals, -1);

        for(int i=0;i<objects.size();i++) {
            if(objects.get(i) != null)
                put(objects.get(i), i);
        }

        this.objects = objects;
    }

    private void put(Object obj, int ordinal) {
        int hash = rehash(System.identityHashCode(obj));

        int bucket = hash & hashModMask;

        while(hashedOrdinals[bucket] != -1)
            bucket = (bucket + 1) & hashModMask;

        hashedOrdinals[bucket] = ordinal;
    }

    public int get(Object obj) {
        int hash = rehash(System.identityHashCode(obj));

        int bucket = hash & hashModMask;

        while(hashedOrdinals[bucket] != -1) {
            if(objects.get(hashedOrdinals[bucket]) == obj)
                return hashedOrdinals[bucket];
            bucket = (bucket + 1) & hashModMask;
        }

        return -1;
    }

    private int rehash(int hash) {
        hash = ~hash + (hash << 15);
        hash = hash ^ (hash >>> 12);
        hash = hash + (hash << 2);
        hash = hash ^ (hash >>> 4);
        hash = hash * 2057;
        hash = hash ^ (hash >>> 16);
        return hash & Integer.MAX_VALUE;
    }
}
