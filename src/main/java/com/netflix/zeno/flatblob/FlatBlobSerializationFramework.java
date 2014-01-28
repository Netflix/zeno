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
package com.netflix.zeno.flatblob;

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.fastblob.record.ByteData;
import com.netflix.zeno.fastblob.record.ByteDataBuffer;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

import java.io.IOException;

public class FlatBlobSerializationFramework extends SerializationFramework {

    FastBlobStateEngine stateEngine;

    public FlatBlobSerializationFramework(SerializerFactory serializerFactory, FastBlobStateEngine readDeserializedObjectsFrom) {
        this(serializerFactory, readDeserializedObjectsFrom, true);
    }

    public FlatBlobSerializationFramework(SerializerFactory serializerFactory, FastBlobStateEngine readDeserializedObjectsFrom, boolean deduplicate) {
        super(serializerFactory);
        this.stateEngine = readDeserializedObjectsFrom;
        this.frameworkSerializer = new FlatBlobFrameworkSerializer(this, stateEngine);
        this.frameworkDeserializer = new FlatBlobFrameworkDeserializer(this, deduplicate);

        ///TODO: The data structure created here is used for double snapshot refresh.  If this is used in a real implementation,
        ///then we would require a separate instance of the identity ordinal map, AND we would need to update this every cycle,
        ///AND make sure this doesn't get out of sync with the actual objects.
        for(String serializerName : stateEngine.getSerializerNames()) {
            stateEngine.getTypeDeserializationState(serializerName).createIdentityOrdinalMap();
        }
    }

    public void serialize(String type, Object obj, ByteDataBuffer os) throws IOException {
        FlatBlobSerializationRecord rec = ((FlatBlobFrameworkSerializer)frameworkSerializer).getSerializationRecord(type);

        getSerializer(type).serialize(obj, rec);

        rec.writeDataTo(os);
    }

    public <T> T deserialize(String type, ByteData data) {
        return deserialize(type, data, 0);
    }

    @SuppressWarnings("unchecked")
    public <T> T deserialize(String type, ByteData data, int position) {
        FlatBlobDeserializationRecord rec = ((FlatBlobFrameworkDeserializer)frameworkDeserializer).getDeserializationRecord(type);
        rec.setByteData(data);
        rec.position(position);

        return (T) getSerializer(type).deserialize(rec);
    }

    public <T> T getCached(String type, int ordinal) {
        FlatBlobTypeCache<T> typeCache = ((FlatBlobFrameworkDeserializer)frameworkDeserializer).getTypeCache(type);
        return typeCache.get(ordinal);
    }

}
