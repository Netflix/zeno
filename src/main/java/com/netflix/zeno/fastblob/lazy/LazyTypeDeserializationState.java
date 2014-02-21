package com.netflix.zeno.fastblob.lazy;

import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.FastBlobDeserializationRecord;
import com.netflix.zeno.fastblob.state.TypeDeserializationState;
import com.netflix.zeno.serializer.NFTypeSerializer;

public class LazyTypeDeserializationState<T> implements TypeDeserializationState<T> {

    private final NFTypeSerializer<T> serializer;
    private final ThreadLocal<FastBlobDeserializationRecord> recHandle;
    private final OrdinalByteArrayMap data;

    private int maxOrdinal = 0;

    public LazyTypeDeserializationState(NFTypeSerializer<T> serializer) {
        this.serializer = serializer;
        this.recHandle = new ThreadLocal<FastBlobDeserializationRecord>();
        this.data = new OrdinalByteArrayMap(256);
    }

    public T get(int ordinal) {
        long pointer = data.getPointer(ordinal);

        if(pointer == -1)
            return null;

        FastBlobDeserializationRecord rec = getRec();
        rec.position((int)pointer);

        return serializer.deserialize(rec);
    }

    public void add(int ordinal, ByteData byteData, long start, int length) {
        data.add(ordinal, byteData, start, length);

        if(ordinal > maxOrdinal)
            maxOrdinal = ordinal;
    }

    public int getMaxOrdinal() {
        return maxOrdinal;
    }

    private FastBlobDeserializationRecord getRec() {
        FastBlobDeserializationRecord rec = recHandle.get();
        if(rec == null) {
            rec = new FastBlobDeserializationRecord(serializer.getFastBlobSchema(), data.getByteData());
            recHandle.set(rec);
        }

        return rec;
    }

}
