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
import java.io.InputStream;

/**
 * This class buffers data from an InputStream.  The buffered data can be accessed randomly within
 * a predefined window before or after the greatest previously accessed byte index.<p/>
 *
 * When using this class, some bytes before and after the maximum byte previously accessed
 * are available via the get() method inherited from ByteData.<p/>
 *
 * Specifically, 2^(log2OfBufferSegmentLength) bytes, both before and after the maximum byte
 * previously accessed by either the stream's read() or ByteData's get() method are available.<p/>
 *
 * This is useful when reading the FastBlob.  Although records are pulled from the FastBlob
 * stream one at a time, the FastBlobDeserializationRecord requires random access to the bytes
 * in the record.<p/>
 *
 * The FastBlobWriter records the ceil(log2(maxLength)) of the individual records contained in the FastBlob.
 * Upon deserialization, this value is read and passed to the constructor of this class to set the buffer length.
 * This guarantees that the reader can access the entire record while it is being read (because the maximum byte
 * accessed while deserializing the record will at most be the last byte of the record).
 *
 * @author dkoszewnik
 *
 */
public class StreamingByteData extends InputStream implements ByteData {

    private final InputStream underlyingStream;
    private final int bufferSegmentLength;
    private final int log2OfBufferSegmentLength;
    private final int bufferSegmentLengthMask;

    private long bufferStartPosition;
    private final byte buf[][];

    private long eofPosition = Long.MAX_VALUE;
    private long currentStreamPosition;

    public StreamingByteData(InputStream in, int log2OfBufferSegmentLength) {
        this.underlyingStream = in;
        this.log2OfBufferSegmentLength = log2OfBufferSegmentLength;
        this.bufferSegmentLength = 1 << log2OfBufferSegmentLength;
        this.bufferSegmentLengthMask = bufferSegmentLength - 1;
        this.buf = new byte[4][];

        for(int i=0;i<4;i++) {
            buf[i] = new byte[bufferSegmentLength];
            if(eofPosition == Long.MAX_VALUE)
                fillArray(buf[i], (bufferSegmentLength * i));
        }
    }

    /**
     * This method provides random-access to the stream data.  To guarantee availability, the position should be no less than
     * the greatest previously accessed byte (via either get() or read()) minus 2^log2OfBufferSegmentLength, and no more
     * than the greatest previously access byte plus 2^log2OfBufferSegmentLength.
     *
     * @param position is the index into the stream data.
     * @return the byte at position.
     */
    @Override
    public byte get(long position) {
        // subtract the buffer start position to get the position in the buffer
        position -= bufferStartPosition;

        // if this position will be reading from the last buffer segment
        if(position >= (bufferSegmentLength * 3)) {
            // move the segments down and fill another segment.
            fillNewBuffer();
            position -= bufferSegmentLength;
        }

        if((int)(position >>> log2OfBufferSegmentLength) < 0 || (int)(position & bufferSegmentLengthMask) < 0)
            System.out.println("found a bug");

        // return the appropriate byte out of the buffer
        return buf[(int)(position >>> log2OfBufferSegmentLength)][(int)(position & bufferSegmentLengthMask)];
    }

    @Override
    public int read() throws IOException {
        // if there are no more bytes, return -1
        if(currentStreamPosition >= eofPosition)
            return -1;

        // use the get method to get the appropriate byte from the stream
        return (int)get(currentStreamPosition++);
    }


    public long currentStreamPosition() {
        return currentStreamPosition;
    }

    /**
     * If bytes should be accessed via the get() method only, this method
     * can be used to skip them in the stream (not return from read()).
     *
     * @param incrementBy how many bytes to "skip", or omit from calls to read()
     */
    public void incrementStreamPosition(int incrementBy) {
        currentStreamPosition += incrementBy;
    }

    /**
     * Close the underlying stream
     */
    @Override
    public void close() throws IOException {
        underlyingStream.close();
    }

    /**
     * Discards the oldest buffer and fills a new buffer
     */
    private void fillNewBuffer() {
        byte temp[] = buf[0];
        buf[0] = buf[1];
        buf[1] = buf[2];
        buf[2] = buf[3];
        buf[3] = temp;

        bufferStartPosition += bufferSegmentLength;

        if(eofPosition == Long.MAX_VALUE)
            fillArray(buf[3], bufferStartPosition + (bufferSegmentLength * 3));
    }

    /**
     * Fills a byte array with data from the underlying stream
     */
    private void fillArray(byte arr[], long segmentStartByte) {
        try {
            int n = 0;
            while (n < arr.length) {
                    int count = underlyingStream.read(arr, n, arr.length - n);

                    // if we have reached the end of the stream, record the byte position at which the stream ends.
                    if (count < 0) {
                        eofPosition = segmentStartByte + n;
                        return;
                    }

                    n += count;
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to read from stream", e);
        }
    }

}
