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
package com.netflix.zeno.fastblob.lazy;

import java.util.Arrays;

/**
 * A segmented long array can grow without allocating successively larger blocks and copying memory.<p/>
 *
 * Segment length is always a power of two so that the location of a given index can be found with mask and shift operations.<p/>
 *
 * Conceptually this can be thought of as a single long array of undefined length.  The currently allocated buffer will always be
 * a multiple of the size of the segments.  The buffer will grow automatically when a byte is written to an index greater than the
 * currently allocated buffer.
 *
 * @author dkoszewnik
 *
 */
public class SegmentedLongArray {

    private long[][] segments;
    private final int log2OfSegmentSize;
    private final int bitmask;

    public SegmentedLongArray(int log2OfSegmentSize) {
        this.segments = new long[2][];
        this.log2OfSegmentSize = log2OfSegmentSize;
        this.bitmask = (1 << log2OfSegmentSize) - 1;
    }

    /**
     * Set the byte at the given index to the specified value
     */
    public void set(long index, long value) {
        int segmentIndex = (int)(index >> log2OfSegmentSize);
        ensureCapacity(segmentIndex);
        segments[segmentIndex][(int)(index & bitmask)] = value;
    }

    /**
     * Get the value of the byte at the specified index.
     */
    public long get(long index) {
        ensureCapacity((int)(index >>> log2OfSegmentSize));
        return segments[(int)(index >>> log2OfSegmentSize)][(int)(index & bitmask)];
    }

    /**
     * Ensures that the segment at segmentIndex exists
     *
     * @param segmentIndex
     */
    private void ensureCapacity(int segmentIndex) {
        while(segmentIndex >= segments.length) {
            segments = Arrays.copyOf(segments, segments.length * 3 / 2);
        }

        if(segments[segmentIndex] == null) {
            segments[segmentIndex] = new long[1 << log2OfSegmentSize];
        }
    }

}
