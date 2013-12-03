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

import static org.junit.Assert.assertEquals;

import com.netflix.zeno.fastblob.record.ByteDataBuffer;

import org.junit.Test;

public class ByteDataBufferTest {

    @Test
    public void recordsData() {
        ByteDataBuffer buf = new ByteDataBuffer(256);

        for(int i=0;i<1000;i++) {
            buf.write((byte)i);
        }

        for(int i=0;i<1000;i++) {
            assertEquals((byte)i, buf.get(i));
        }
    }

    @Test
    public void canBeReset() {
        ByteDataBuffer buf = new ByteDataBuffer(256);

        for(int i=0;i<1000;i++) {
            buf.write((byte)10);
        }

        buf.reset();

        for(int i=0;i<1000;i++) {
            buf.write((byte)i);
        }

        for(int i=0;i<1000;i++) {
            assertEquals((byte)i, buf.get(i));
        }
    }

}
