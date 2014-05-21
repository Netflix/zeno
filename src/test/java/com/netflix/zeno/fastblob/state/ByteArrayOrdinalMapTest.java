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

import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.fastblob.record.VarInt;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class ByteArrayOrdinalMapTest {

    @Test
    public void compacts() {
        ByteArrayOrdinalMap map = new ByteArrayOrdinalMap();

        ByteDataBuffer buf = new ByteDataBuffer();

        /// add 1000 entries
        for(int i=0;i<1000;i++) {
            VarInt.writeVInt(buf, 10000 + i);
            map.getOrAssignOrdinal(buf);
            buf.reset();
        }


        /// mark half of the entries used
        ThreadSafeBitSet bitSet = new ThreadSafeBitSet(10);
        for(int i=0;i<1000;i+=2) {
            bitSet.set(i);
        }

        /// compact away the unused entries
        map.compact(bitSet);

        /// ensure that the used entries are still available
        for(int i=0;i<1000;i+=2) {
            VarInt.writeVInt(buf, 10000 + i);
            Assert.assertEquals(i, map.getOrAssignOrdinal(buf));
            buf.reset();
        }


        /// track the ordinals which are assigned to new values
        Set<Integer> newlyAssignedOrdinals = new HashSet<Integer>();
        for(int i=1;i<1000;i+=2) {
            VarInt.writeVInt(buf, 50230532 + i);
            int newOrdinal = map.getOrAssignOrdinal(buf);
            newlyAssignedOrdinals.add(newOrdinal);
            buf.reset();
        }

        /// those ordinals should be the recycled ones after the compact.
        for(int i=1;i<1000;i+=2) {
            Assert.assertTrue(newlyAssignedOrdinals.contains(i));
        }

    }

    @Test
    public void clientSideHeapSafeUsageTest() {
        ByteArrayOrdinalMap map = new ByteArrayOrdinalMap();

        ByteDataBuffer buf = new ByteDataBuffer();

        for(int i=0;i<1000;i++) {
            VarInt.writeVInt(buf, 10000 + i);
            map.put(buf, i + 50);
            buf.reset();
        }

        for(int i=0;i<1000;i++) {
            VarInt.writeVInt(buf, 10000 + i);
            Assert.assertEquals(i + 50, map.get(buf));
            buf.reset();
        }

        for(int i=0;i<1000;i++) {
            VarInt.writeVInt(buf, 20000 + i);
            Assert.assertEquals(-1, map.get(buf));
            buf.reset();
        }

        map.clear();

        Assert.assertEquals(0, map.getDataSize());

        for(int i=0;i<5000;i++) {
            VarInt.writeVInt(buf, 20000 + i);
            Assert.assertEquals(-1, map.get(buf));
            buf.reset();
        }

        for(int i=0;i<5000;i++) {
            VarInt.writeVInt(buf, 20000 + i);
            map.put(buf, i + 50);
            buf.reset();
        }

    }

    @Test
    public void testThreadSafety() throws IOException {
        int numThreads = 100;
        final int numUniqueValues = 1000000;
        final int numIterationsPerThread = 200000;

        ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

        final ConcurrentHashMap<Integer, Integer> controlMap = new ConcurrentHashMap<Integer, Integer>();
        final ByteArrayOrdinalMap map = new ByteArrayOrdinalMap();

        for(int i=0;i<numThreads;i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    Random rand = new Random();
                    ByteDataBuffer buf = new ByteDataBuffer();

                    for(int i=0;i<numIterationsPerThread;i++) {
                        int value = rand.nextInt(numUniqueValues);

                        VarInt.writeVInt(buf, value);

                        Integer ordinal = Integer.valueOf(map.getOrAssignOrdinal(buf));

                        Integer beatMe = controlMap.putIfAbsent(value, ordinal);

                        if(beatMe != null) {
                            Assert.assertEquals(ordinal, beatMe);
                        }

                        buf.reset();
                    }

                }
            });

        }

        shutdown(executor);

        /// serialize then deserialize the map
        /*ByteArrayOutputStream os = new ByteArrayOutputStream();
        map.serializeTo(os);
        ByteArrayOrdinalMap deserializedMap = ByteArrayOrdinalMap.deserializeFrom(new ByteArrayInputStream(os.toByteArray()));*/

        ByteDataBuffer buf = new ByteDataBuffer();

        for(Map.Entry<Integer, Integer> entry : controlMap.entrySet()) {
            Integer value = entry.getKey();
            Integer expected = entry.getValue();

            buf.reset();
            VarInt.writeVLong(buf, value.longValue());

            int actual = map.getOrAssignOrdinal(buf);

            Assert.assertEquals(actual, expected.intValue());
        }

    }

    private void shutdown(ThreadPoolExecutor executor) {
        executor.shutdown();
        while(!executor.isTerminated()) {
            try {
                executor.awaitTermination(1, TimeUnit.DAYS);
            } catch (final InterruptedException e) { }
        }
    }

}
