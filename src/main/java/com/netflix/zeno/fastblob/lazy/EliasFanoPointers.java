/*
 *
 *  Copyright 2014 Netflix, Inc.
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
package com.netflix.zeno.fastblob.lazy;

/**
 * Quasi-succinct indices:<br/>
 * http://vigna.di.unimi.it/ftp/papers/QuasiSuccinctIndices.pdf
 *
 * Thanks to Sebastiano Vigna and Massimo Santini, of the LAW lab at the Università degli studi di Milano,
 * for pointing in this direction after reading the NetflixGraph tech blog.
 *
 * @author dkoszewnik
 *
 */
public class EliasFanoPointers {

    private int currentIndex;
    private long previousValue;

    private final long skipIndex[];

    private final SegmentedLongArray highBits;
    private long previousHighBitValue;
    private long currentHighBit;

    private final long lowBits[];
    private final int numLowBits;
    private final long lowBitsMask;

    public EliasFanoPointers(long maxPointer, int numPointers){
        this.skipIndex = new long[((numPointers-1) / 64) + 2];
        this.highBits = new SegmentedLongArray(5);
        this.numLowBits = 31 - Integer.numberOfLeadingZeros((int) (maxPointer / numPointers));
        this.lowBitsMask = (1 << numLowBits) - 1;
        this.lowBits = new long[((numPointers * numLowBits) / 64) + 1];
        this.previousValue = -1;
        this.previousHighBitValue = -1;
    }

    public long get(int index) {
        long highBit = skipIndex[(index >>> 7) * 2];
        long previousValue = skipIndex[((index >>> 7) * 2) + 1];

        long highBitValue = previousValue >> numLowBits;

        int currentHighBitLongIndex = (int)(highBit >>> 6);
        int currentHighBitBitIndex = (int)(highBit & 0x3F);

        int numBitsToRead = index & 0x7F;

        long curVal = highBits.get(currentHighBitLongIndex);
        curVal >>>= currentHighBitBitIndex;
        int bitCount = Long.bitCount(curVal);

        while(numBitsToRead >= bitCount) {
            highBitValue += 64 - currentHighBitBitIndex - bitCount;
            numBitsToRead -= bitCount;
            currentHighBitBitIndex = 0;
            currentHighBitLongIndex++;
            curVal = highBits.get(currentHighBitLongIndex);
            bitCount = Long.bitCount(curVal);
        }

        int encodedHighBitValue = 0;

        for(int i=0;i<=numBitsToRead;i++) {
            encodedHighBitValue = Long.numberOfTrailingZeros(curVal);
            highBitValue += encodedHighBitValue;
            curVal >>>= encodedHighBitValue + 1;
        }

        long lowBitData = getLowBits(index);

        if(lowBitData == 0 && encodedHighBitValue == 0 &&
                (numBitsToRead != 0 || currentHighBitBitIndex != 0 ||
                (currentHighBitLongIndex == 0 || highBits.get(currentHighBitLongIndex - 1) < 0))) {
            return -1;
        }

        return (highBitValue << numLowBits) | lowBitData;
    }

    public void add(long pointer) {
        if((currentIndex & 0x7F) == 0) {
            skipIndex[currentIndex >>> 6] = currentHighBit;
            skipIndex[(currentIndex >>> 6) + 1] = previousValue;
        }

        if(pointer == -1) {
            addValueIntoHighBits(0);
            currentIndex++;
        } else {
            setLowBits(currentIndex, pointer);
            long highBitValue = pointer >>> numLowBits;
            addValueIntoHighBits(highBitValue - previousHighBitValue);
            previousHighBitValue = highBitValue;
            previousValue = pointer;
            currentIndex++;
        }
    }

    void addValueIntoHighBits(long value) {
        currentHighBit += value;
        int longIndex = (int)(currentHighBit >> 6);
        int bitIndex = (int)(currentHighBit & 0x3F);

        long l = highBits.get(longIndex);
        highBits.set(longIndex, l | (1L << bitIndex));
        currentHighBit++;
    }

    void setLowBits(int index, long lowBits) {
        lowBits &= lowBitsMask;
        long bitIndex = index * numLowBits;
        int lowBitsLongIndex = (int)(bitIndex >>> 6);
        int lowBitsBitIndex = (int)(bitIndex & 0x3F);

        this.lowBits[lowBitsLongIndex] |= lowBits << lowBitsBitIndex;

        if(lowBitsBitIndex >= (64 - numLowBits)) {
            this.lowBits[lowBitsLongIndex + 1] |= lowBits >>> (64-lowBitsBitIndex);
        }
    }

    long getLowBits(int index) {
        long bitIndex = index * numLowBits;
        int lowBitsLongIndex = (int)(bitIndex >>> 6);
        int lowBitsBitIndex = (int)(bitIndex & 0x3F);

        if(lowBitsBitIndex < (64 - numLowBits)) {
            return (lowBits[lowBitsLongIndex] >>> lowBitsBitIndex) & lowBitsMask;
        } else {
            return ((lowBits[lowBitsLongIndex] >>> lowBitsBitIndex) | (lowBits[lowBitsLongIndex + 1] << (64 - lowBitsBitIndex))) & lowBitsMask;
        }
    }

    long numLongStorageRequirement() {
        return skipIndex.length + ((currentHighBit >> 6) + 1) + lowBits.length;
    }

}
