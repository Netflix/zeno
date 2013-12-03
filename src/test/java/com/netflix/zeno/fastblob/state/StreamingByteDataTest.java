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

import com.netflix.zeno.fastblob.record.StreamingByteData;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StreamingByteDataTest {

    private StreamingByteData data;

    @Before
    public void setUp() {
        byte arr[] = new byte[100];

        for(int i=0;i<arr.length;i++) {
            arr[i] = (byte)i;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(arr);

        data = new StreamingByteData(bais, 4);
    }


    @Test
    public void canBeUsedAsAStream() throws IOException {
        for(int i=0;i<100;i++) {
            Assert.assertEquals((int)i, data.read());
        }

        Assert.assertEquals(-1, data.read());
    }

    @Test
    public void canBeUsedAsASegmentedByteArray() {
        for(int i=0;i<100;i++) {
            Assert.assertEquals((int)i, data.get(i));
            if(i > 16)
                Assert.assertEquals((int)i-16, data.get(i-16));
            if(i < 84)
                Assert.assertEquals((int)i+16, data.get(i+16));
        }
    }

    @Test
    public void canBeUsedAsSegmentedByteArrayAndStream() throws IOException {
        for(int i=0;i<100;i++) {
            if(i % 2 == 0) {
                Assert.assertEquals((int)i, data.get(i));
                data.incrementStreamPosition(1);
            } else {
                Assert.assertEquals((int)i, data.read());
            }
        }

        Assert.assertEquals(-1, data.read());
    }

}
