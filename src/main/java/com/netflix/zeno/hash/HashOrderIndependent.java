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
package com.netflix.zeno.hash;

import java.io.IOException;

/**
 * OrderIndependent implementation of the hashing algorithm
 *
 * @author tvaliulin
 */
public final class HashOrderIndependent implements HashAlgorithm {
    // TODO : increase capacity of the accumulator by at least one more long
    long hashCode = 0;

    /**
     * Constructor that takes in the OutputStream that we are wrapping and
     * creates the MD5 MessageDigest
     */
    public HashOrderIndependent() {
        super();

    }

    private static long hash(long h) {
        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.netflix.videometadata.serializer.blob.HashAlgorithm#write(char)
     */
    @Override
    public void write(char b) throws IOException {
        write("char");
        write((long) b);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(boolean)
     */
    @Override
    public void write(boolean b) throws IOException {
        write("boolean");
        write((b ? 0xf00bf00b : 0xf81bc437));
    }

    /*
     * (non-Javadoc)
     *
     * @see com.netflix.videometadata.serializer.blob.HashAlgorithm#write(long)
     */
    @Override
    public void write(long b) throws IOException {
        write("long");
        hashCode = hashCode + hash(b);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.netflix.videometadata.serializer.blob.HashAlgorithm#write(float)
     */
    @Override
    public void write(float b) throws IOException {
        write("float");
        write(Float.floatToIntBits(b));
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(double)
     */
    @Override
    public void write(double b) throws IOException {
        write("double");
        write(Double.doubleToLongBits(b));
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(java.lang
     * .String)
     */
    @Override
    public void write(String b) throws IOException {
        long code = 0;
        for (int i = 0; i < b.length(); i++) {
            code = 31 * code + b.charAt(i);
        }
        hashCode += hash(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(byte[])
     */
    @Override
    public void write(byte[] b) throws IOException {
        write("byte[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + b[i];
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(char[])
     */
    @Override
    public void write(char[] b) throws IOException {
        write("char[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + b[i];
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(boolean[])
     */
    @Override
    public void write(boolean[] b) throws IOException {
        write("boolean[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + (b[i] ? 2 : 1);
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(short[])
     */
    @Override
    public void write(short[] b) throws IOException {
        write("short[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + b[i];
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.netflix.videometadata.serializer.blob.HashAlgorithm#write(int[])
     */
    @Override
    public void write(int[] b) throws IOException {
        write("int[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + b[i];
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(long[])
     */
    @Override
    public void write(long[] b) throws IOException {
        write("long[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + b[i];
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(float[])
     */
    @Override
    public void write(float[] b) throws IOException {
        write("float[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + Float.floatToIntBits(b[i]);
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.netflix.videometadata.serializer.blob.HashAlgorithm#write(double[])
     */
    @Override
    public void write(double[] b) throws IOException {
        write("double[]");
        long code = 0;
        for (int i = 0; i < b.length; i++) {
            code = 31 * code + Double.doubleToLongBits(b[i]);
        }
        write(code);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.netflix.videometadata.serializer.blob.HashAlgorithm#bytes()
     */
    @Override
    public byte[] bytes() {
        return new byte[] { (byte) ((hashCode >> 56) & 0xff), (byte) ((hashCode >> 48) & 0xff), (byte) ((hashCode >> 40) & 0xff), (byte) ((hashCode >> 32) & 0xff), (byte) ((hashCode >> 24) & 0xff), (byte) ((hashCode >> 16) & 0xff), (byte) ((hashCode >> 8) & 0xff), (byte) ((hashCode >> 0) & 0xff), };
    }
}
