/*
 *
 *  Copyright 2014 Netflix, Inc.
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

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.record.SegmentedByteArray;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.fastblob.state.TypeDeserializationState;
import com.netflix.zeno.serializer.NFTypeSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class LazyTypeDeserializationState<T> implements TypeDeserializationState<T> {

    private final NFTypeSerializer<T> serializer;

    private int size;

    private EliasFanoPointers pointers;
    private Map<Integer, Long> pointersScratch = new HashMap<Integer, Long>();

    private final SegmentedByteArray byteData;
    private long dataLength = 0;

    private int maxOrdinal = -1;

    private FastBlobSchema fastBlobSchema;

    public LazyTypeDeserializationState(NFTypeSerializer<T> serializer) {
        this.serializer = serializer;
        pointersScratch = new HashMap<Integer, Long>();
        this.byteData = new SegmentedByteArray(13);

    }



    public T get(int ordinal) {
        long pointer = pointers.get(ordinal);

        /*long endPointer = dataLength;
        int currentOrdinal = ordinal + 1;
        while(currentOrdinal < maxOrdinal) {
            long nextPointer = pointers.get(currentOrdinal);
            if(nextPointer != -1) {
                endPointer = nextPointer;
                break;
            }
            currentOrdinal++;
        }

        SegmentedByteArray scratch = getScratch();
        scratch.copy(byteData, pointer, 0, (int)(endPointer - pointer));*/

        if(pointer == -1)
            return null;

        FastBlobDeserializationRecord rec = getRec();
        rec.position(pointer);

        return serializer.deserialize(rec);
    }

    public void add(int ordinal, ByteData byteData, long start, int length) {
        pointersScratch.put(ordinal, dataLength);
        copyData(byteData, start, length);

        if(ordinal > maxOrdinal)
            maxOrdinal = ordinal;

        size++;
    }


    private void copyData(ByteData data, long start, int length) {
        long end = start + length;
        for(long i=start;i<end;i++) {
            byteData.set((int)(dataLength++), data.get((int)i));
        }
    }


    public int getMaxOrdinal() {
        return maxOrdinal;
    }

    public void finalizePointers() {
        if(dataLength > 0) {
            List<Map.Entry<Integer, Long>> pointerEntries = new ArrayList<Map.Entry<Integer,Long>>(pointersScratch.entrySet());

            Collections.sort(pointerEntries, new Comparator<Map.Entry<Integer, Long>>() {
                public int compare(Entry<Integer, Long> o1, Entry<Integer, Long> o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
            });

            pointers = new EliasFanoPointers(dataLength, maxOrdinal + 1);
            int currentOrdinal = 0;

            for(Map.Entry<Integer, Long> entry : pointerEntries) {
                while(currentOrdinal < entry.getKey().intValue()) {
                    pointers.add(-1);
                    currentOrdinal++;
                }

                pointers.add(entry.getValue().longValue());

                currentOrdinal++;
            }

        }
        pointersScratch = null;
    }

    private final ThreadLocal<SegmentedByteArray> scratchHandle = new ThreadLocal<SegmentedByteArray>();

    private SegmentedByteArray getScratch() {
        SegmentedByteArray arr = scratchHandle.get();
        if(arr == null) {
            arr = new SegmentedByteArray(5);
            scratchHandle.set(arr);
        }
        return arr;
    }

    private final ThreadLocal<FastBlobDeserializationRecord> recHandle = new ThreadLocal<FastBlobDeserializationRecord>();

    private FastBlobDeserializationRecord getRec() {
        FastBlobDeserializationRecord rec = recHandle.get();
        if(rec == null) {
            rec = new FastBlobDeserializationRecord(fastBlobSchema, byteData);
            recHandle.set(rec);
        }

        return rec;
    }

    public FastBlobSchema getFastBlobSchema() {
        return fastBlobSchema;
    }

    public void setFastBlobSchema(FastBlobSchema fastBlobSchema) {
        this.fastBlobSchema = fastBlobSchema;
    }

}