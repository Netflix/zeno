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
package com.netflix.zeno.fastblob.record;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * A segmented byte array can grow without allocating successively larger blocks and copying memory.<p/>
 *
 * Segment length is always a power of two so that the location of a given index can be found with mask and shift operations.<p/>
 *
 * Conceptually this can be thought of as a single byte array of undefined length.  The currently allocated buffer will always be
 * a multiple of the size of the segments.  The buffer will grow automatically when a byte is written to an index greater than the
 * currently allocated buffer.
 *
 * @author dkoszewnik
 *
 */
public class SegmentedByteArray implements ByteData {

    private byte[][] segments;
    private final int log2OfSegmentSize;
    private final int bitmask;

    public SegmentedByteArray(int log2OfSegmentSize) {
        this.segments = new byte[2][];
        this.log2OfSegmentSize = log2OfSegmentSize;
        this.bitmask = (1 << log2OfSegmentSize) - 1;
    }

    /**
     * Set the byte at the given index to the specified value
     */
    public void set(long index, byte value) {
        int segmentIndex = (int)(index >> log2OfSegmentSize);
        ensureCapacity(segmentIndex);
        segments[segmentIndex][(int)(index & bitmask)] = value;
    }

    /**
     * Get the value of the byte at the specified index.
     */
    public byte get(long index) {
        return segments[(int)(index >>> log2OfSegmentSize)][(int)(index & bitmask)];
    }

    /**
     * Copy bytes from another ByteData to this array.
     *
     * @param src the source data
     * @param srcPos the position to begin copying from the source data
     * @param destPos the position to begin writing in this array
     * @param length the length of the data to copy
     */
    public void copy(ByteData src, long srcPos, long destPos, long length) {
        for(long i=0;i<length;i++) {
            set(destPos++, src.get(srcPos++));
        }
    }

    /**
     * For a SegmentedByteArray, this is a faster copy implementation.
     *
     * @param src
     * @param srcPos
     * @param destPos
     * @param length
     */
    public void copy(SegmentedByteArray src, long srcPos, long destPos, long length) {
        int segmentLength = 1 << log2OfSegmentSize;
        int currentSegment = (int)(destPos >>> log2OfSegmentSize);
        int segmentStartPos = (int)(destPos & bitmask);
        int remainingBytesInSegment = segmentLength - segmentStartPos;

        while(length > 0) {
            int bytesToCopyFromSegment = (int)Math.min(remainingBytesInSegment, length);
            ensureCapacity(currentSegment);
            int copiedBytes = src.copy(srcPos, segments[currentSegment], segmentStartPos, bytesToCopyFromSegment);

            srcPos += copiedBytes;
            length -= copiedBytes;
            segmentStartPos = 0;
            remainingBytesInSegment = segmentLength;
            currentSegment++;
        }

    }

    /**
     * copies exactly data.length bytes from this SegmentedByteArray into the provided byte array
     *
     * @param index
     * @param data
     * @return the number of bytes copied
     */
    public int copy(long srcPos, byte[] data, int destPos, int length) {
        int segmentSize = 1 << log2OfSegmentSize;
        int remainingBytesInSegment = (int)(segmentSize - (srcPos & bitmask));
        int dataPosition = destPos;

        while(length > 0) {
            byte[] segment = segments[(int)(srcPos >>> log2OfSegmentSize)];

            int bytesToCopyFromSegment = Math.min(remainingBytesInSegment, length);

            System.arraycopy(segment, (int)(srcPos & bitmask), data, dataPosition, bytesToCopyFromSegment);

            dataPosition += bytesToCopyFromSegment;
            srcPos += bytesToCopyFromSegment;
            remainingBytesInSegment = segmentSize - (int)(srcPos & bitmask);
            length -= bytesToCopyFromSegment;
        }

        return dataPosition - destPos;
    }

    public void readFrom(RandomAccessFile file, long pointer, int length) throws IOException {
        file.seek(pointer);
        int segmentSize = 1 << log2OfSegmentSize;
        int segment = 0;
        while(length > 0) {
            ensureCapacity(segment);
            int bytesToCopy = Math.min(segmentSize, length);
            int bytesCopied = 0;
            while(bytesCopied < bytesToCopy){
                bytesCopied += file.read(segments[segment], bytesCopied, (bytesToCopy - bytesCopied));
            }
            segment++;
            length -= bytesCopied;
        }
    }

    /**
     * Write a portion of this data to an OutputStream.
     */
    public void writeTo(OutputStream os, long startPosition, long len) throws IOException {
        int segmentSize = 1 << log2OfSegmentSize;
        int remainingBytesInSegment = segmentSize - (int)(startPosition & bitmask);
        long remainingBytesInCopy = len;

        while(remainingBytesInCopy > 0) {
            long bytesToCopyFromSegment = Math.min(remainingBytesInSegment, remainingBytesInCopy);

            os.write(segments[(int)(startPosition >>> log2OfSegmentSize)], (int)(startPosition & bitmask), (int)bytesToCopyFromSegment);

            startPosition += bytesToCopyFromSegment;
            remainingBytesInSegment = segmentSize - (int)(startPosition & bitmask);
            remainingBytesInCopy -= bytesToCopyFromSegment;
        }
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
            segments[segmentIndex] = new byte[1 << log2OfSegmentSize];
        }
    }

}
