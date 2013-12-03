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

import com.netflix.zeno.fastblob.state.FreeOrdinalTracker;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class FreeOrdinalTrackerTest {

    @Test
    public void returnsIncreasingOrdinals() {
        FreeOrdinalTracker tracker = new FreeOrdinalTracker();

        for(int i=0;i<100;i++) {
            Assert.assertEquals(i, tracker.getFreeOrdinal());
        }
    }

    @Test
    public void returnsOrdinalsPreviouslyReturnedToPool() {
        FreeOrdinalTracker tracker = new FreeOrdinalTracker();

        for(int i=0;i<100;i++) {
            tracker.getFreeOrdinal();
        }

        tracker.returnOrdinalToPool(20);
        tracker.returnOrdinalToPool(30);
        tracker.returnOrdinalToPool(40);

        Assert.assertEquals(40, tracker.getFreeOrdinal());
        Assert.assertEquals(30, tracker.getFreeOrdinal());
        Assert.assertEquals(20, tracker.getFreeOrdinal());
        Assert.assertEquals(100, tracker.getFreeOrdinal());
        Assert.assertEquals(101, tracker.getFreeOrdinal());
    }

    @Test
    public void serializesAndDeserializes() throws IOException {
        FreeOrdinalTracker tracker = new FreeOrdinalTracker();

        for(int i=0;i<100;i++) {
            tracker.getFreeOrdinal();
        }

        tracker.returnOrdinalToPool(20);
        tracker.returnOrdinalToPool(30);
        tracker.returnOrdinalToPool(40);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        tracker.serializeTo(baos);

        FreeOrdinalTracker deserializedTracker = FreeOrdinalTracker.deserializeFrom(new ByteArrayInputStream(baos.toByteArray()));

        Assert.assertEquals(40, deserializedTracker.getFreeOrdinal());
        Assert.assertEquals(30, deserializedTracker.getFreeOrdinal());
        Assert.assertEquals(20, deserializedTracker.getFreeOrdinal());
        Assert.assertEquals(100, deserializedTracker.getFreeOrdinal());
        Assert.assertEquals(101, deserializedTracker.getFreeOrdinal());
    }

}
