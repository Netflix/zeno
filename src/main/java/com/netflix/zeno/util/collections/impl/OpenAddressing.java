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
package com.netflix.zeno.util.collections.impl;

import java.util.Arrays;

/**
 * Common functionality for the Open Addressing HashSet and HashMaps.
 *
 * @author dkoszewnik
 *
 */
public class OpenAddressing {
    /*
     * The number of entries will determine the size of the hash table elements.
     *
     * If the entry index can be always represented in 8 bits, the hash table
     * will be byte[] If the entry index can always be represented in 16 bits,
     * the hash table will be short[] Otherwise, the hash table will be int[].
     *
     * The sign bit is used for byte[] and short[] hash tables, but the value -1
     * is reserved to mean "empty".
     *
     * Because the entries[] array stores both keys and values, the key for
     * entry "n" will be stored at entries[n*2]. "n" is the value stored in the
     * hash table.
     */
    public static Object newHashTable(int numEntries, float loadFactor) {

        int hashSize = (int) Math.ceil((float) numEntries / loadFactor);
        hashSize = 1 << (32 - Integer.numberOfLeadingZeros(hashSize)); // next
                                                                       // power
                                                                       // of 2

        if (numEntries < 256) {
            byte hashTable[] = new byte[hashSize];
            Arrays.fill(hashTable, (byte) -1);
            return hashTable;
        }

        if (numEntries < 65536) {
            short hashTable[] = new short[hashSize];
            Arrays.fill(hashTable, (short) -1);
            return hashTable;
        }

        int hashTable[] = new int[hashSize];
        Arrays.fill(hashTable, -1);
        return hashTable;
    }

    // the type of primitives used to represent the hash table entries pivots
    // based on the necessary bits
    public static int hashTableLength(Object hashTable) {
        if (hashTable instanceof byte[]) {
            return ((byte[]) hashTable).length;
        } else if (hashTable instanceof short[]) {
            return ((short[]) hashTable).length;
        }
        return ((int[]) hashTable).length;
    }

    // / the type of primitives used to represent the hash table entries pivots
    // based on the necessary bits
    public static int getHashEntry(Object hashTable, int bucket) {
        if (hashTable instanceof byte[]) {
            int entry = ((byte[]) hashTable)[bucket] & 0xFF;
            return entry == 0xFF ? -1 : entry;
        }
        if (hashTable instanceof short[]) {
            int entry = ((short[]) hashTable)[bucket] & 0xFFFF;
            return entry == 0xFFFF ? -1 : entry;
        }
        return ((int[]) hashTable)[bucket];
    }

    // the type of primitives used to represent the hash table entries pivots
    // based on the necessary bits
    public static void setHashEntry(Object hashTable, int bucket, int value) {
        if (hashTable instanceof byte[]) {
            ((byte[]) hashTable)[bucket] = (byte) value;
        } else if (hashTable instanceof short[]) {
            ((short[]) hashTable)[bucket] = (short) value;
        } else {
            ((int[]) hashTable)[bucket] = value;
        }
    }

    // this is Thomas Wang's commonly used 32-bit hash function.
    // it gives a very good balance between CPU and distribution of
    // keys. This is extremely important when using open-addressed hashing
    // like we are in this Map structure.
    public static int rehash(int hash) {
        hash = ~hash + (hash << 15);
        hash = hash ^ (hash >>> 12);
        hash = hash + (hash << 2);
        hash = hash ^ (hash >>> 4);
        hash = hash * 2057;
        hash = hash ^ (hash >>> 16);
        return hash;
    }

}
