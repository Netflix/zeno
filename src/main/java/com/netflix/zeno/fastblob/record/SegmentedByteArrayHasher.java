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

/**
 *  This class performs a fast murmurhash3 on a sequence of bytes.<p/>
 *
 *  MurmurHash is a high quality hash algorithm for byte data:<p/>
 *
 *  http://en.wikipedia.org/wiki/MurmurHash
 */
public class SegmentedByteArrayHasher {

    private static final int SEED = 0xeab524b9;

    public static int hashCode(ByteDataBuffer data) {
        return hashCode(data.getUnderlyingArray(), 0, (int)data.length());
    }

    /**
     * MurmurHash3.  Adapted from:<p/>
     *
     * https://github.com/yonik/java_util/blob/master/src/util/hash/MurmurHash3.java<p/>
     *
     * On 11/19/2013 the license for this file read:<p/>
     *
     *  The MurmurHash3 algorithm was created by Austin Appleby.  This java port was authored by
     *  Yonik Seeley and is placed into the public domain.  The author hereby disclaims copyright
     *  to this source code.
     *  <p>
     *  This produces exactly the same hash values as the final C++
     *  version of MurmurHash3 and is thus suitable for producing the same hash values across
     *  platforms.
     *  <p>
     *  The 32 bit x86 version of this hash should be the fastest variant for relatively short keys like ids.
     *  <p>
     *  Note - The x86 and x64 versions do _not_ produce the same results, as the
     *  algorithms are optimized for their respective platforms.
     *  <p>
     *  See http://github.com/yonik/java_util for future updates to this file.
     *
     */
    ///
    public static int hashCode(ByteData data, long offset, int len) {

        final int c1 = 0xcc9e2d51;
        final int c2 = 0x1b873593;

        int h1 = SEED;
        long roundedEnd = offset + (len & 0xfffffffffffffffcL); // round down to 4 byte
                                                      // block

        for (long i = offset; i < roundedEnd; i += 4) {
            // little endian load order
            int k1 = (data.get(i) & 0xff) | ((data.get(i + 1) & 0xff) << 8) | ((data.get(i + 2) & 0xff) << 16) | (data.get(i + 3) << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19); // ROTL32(h1,13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        // tail
        int k1 = 0;

        switch (len & 0x03) {
        case 3:
            k1 = (data.get(roundedEnd + 2) & 0xff) << 16;
            // fallthrough
        case 2:
            k1 |= (data.get(roundedEnd + 1) & 0xff) << 8;
            // fallthrough
        case 1:
            k1 |= (data.get(roundedEnd) & 0xff);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17); // ROTL32(k1,15);
            k1 *= c2;
            h1 ^= k1;
        }

        // finalization
        h1 ^= len;

        // fmix(h1);
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }

}
