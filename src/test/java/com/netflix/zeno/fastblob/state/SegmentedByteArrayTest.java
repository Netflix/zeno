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

import com.netflix.zeno.fastblob.record.SegmentedByteArray;

import org.junit.Assert;
import org.junit.Test;

public class SegmentedByteArrayTest {

    @Test
    public void noSuchThingAsOutOfBoundsExceptionWhenWriting() {
        SegmentedByteArray arr = new SegmentedByteArray(3); // each segment is size 8

        for(int i=0;i<1000;i++) {
            arr.set(i, (byte)i);
        }

        for(int i=0;i<1000;i++) {
            Assert.assertEquals((byte)i, arr.get(i));
        }
    }

    @Test
    public void copyToAnotherSegmentedByteArray() {
        SegmentedByteArray src = new SegmentedByteArray(3);

        for(int i=0;i<128;i++) {
            src.set(i, (byte)i);
        }

        SegmentedByteArray dest = new SegmentedByteArray(3);

        for(int i=0;i<(128-32);i++) {
            for(int j=0;j<(128-32);j++) {
                dest.copy(src, i, j, 32);

                for(int k=0;k<32;k++) {
                    Assert.assertEquals((byte)src.get(i+k), (byte)dest.get(j+k));
                }
            }
        }
    }

    @Test
    public void canReferenceLongSpace() {
        SegmentedByteArray arr = new SegmentedByteArray(25);

        arr.set(0x1FFFFFFFFL, (byte)100);

        Assert.assertEquals((byte)100, arr.get(0x1FFFFFFFFL));
    }

}
