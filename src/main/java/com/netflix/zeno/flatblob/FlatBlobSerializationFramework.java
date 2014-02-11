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

/**
 * The "flat blob" is currently an experiment.  We are challenging the assumption that all Netflix applications require
 * all video metadata in memory at any given time.<p/>
 *
 * For at least some of our applications, we observe a power-law distribution of accesses by key.  We believe that we may
 * be able to maximize the value of the cache by only storing in memory the most frequently accessed items.  The rest of the items
 * can be stored off-heap somewhere.<p/>
 *
 * Whether "somewhere" is on disk or on a separate server, the cost of retrieving data will be dominated by the back-and-forth time
 * to the off-heap repository, not the amount of data returned.  Consequently, we need to be able to retrieve an entire hierarchy of
 * each object in a single request (rather than making piecewise calls off-heap for each sub-element).<p/>
 *
 * This is where the "flat blob" comes in.  The "flat blob" representation includes (in roughly FastBlob format), each of the data elements
 * which are referenced by a given object.<p/>
 *
 * Experiments have shown that even in a partial cache, deduplication still has enormous value (even caching just the most frequently
 * accessed 10,000 items, FastBlob-style deduplication results in a 71% reduction in memory footprint).<p/>
 *
 * Let's take and example object OBJ, which references three sub-objects O1, O2, and O3, with the ordinals 0, 1, and 2, respectively.<p/>
 *
 * The FastBlob serialization is [OBJ] = "012"<br/>
 * The FlatBlob serialization is [OBJ] = "0[O1]1[O2]2[O3]"<p/>
 *
 * Where the FastBlob serialization format includes only ordinal references for sub-elements, the "flat blob" serialization format includes <i>both</i>
 * the ordinal reference and the complete serialized representation of those sub-elements.<p/>
 *
 * The deserializer can optionally cache these intermediate objects.  When reading this data, the deserializer will check to see whether each intermediate
 * object is cached.  If so, it will use the cached copy.  If not, it will deserialize and then optionally cache the sub-object.  This results in
 * the reduced allocation, promotion and memory footprint enjoyed by the FastBlobStateEngine. <p/>
 *
 * The FlatBlob builds on the FastBlobStateEngine foundation by retaining the concepts of data states and ordinals.  In this way, we
 * can retain the memory footprint and GC overhead benefits of FastBlob-style deduplication, while simultaneously optimizing for higher-latency
 * off-heap data access.
 *
 * @author dkoszewnik
 *
 */
public class FlatBlobSerializationFramework extends SerializationFramework {

    FastBlobStateEngine stateEngine;

    public FlatBlobSerializationFramework(SerializerFactory serializerFactory) {
        this(serializerFactory, null);
    }

    public FlatBlobSerializationFramework(SerializerFactory serializerFactory, FastBlobStateEngine readDeserializedObjectsFrom) {
        super(serializerFactory);
        this.stateEngine = readDeserializedObjectsFrom;
        this.frameworkSerializer = new FlatBlobFrameworkSerializer(this, stateEngine);
        this.frameworkDeserializer = new FlatBlobFrameworkDeserializer(this);

        ///TODO: The data structure created here is used for double snapshot refresh.  If this is used in a real implementation,
        ///then we would require a separate instance of the identity ordinal map, AND we would need to update this every cycle,
        ///AND make sure this doesn't get out of sync with the actual objects.
        if(stateEngine != null) {
            for(String serializerName : stateEngine.getSerializerNames()) {
                stateEngine.getTypeDeserializationState(serializerName).createIdentityOrdinalMap();
            }
        }
    }

    public void serialize(String type, Object obj, ByteDataBuffer os) {
        FlatBlobSerializationRecord rec = ((FlatBlobFrameworkSerializer)frameworkSerializer).getSerializationRecord(type);

        getSerializer(type).serialize(obj, rec);

        rec.writeDataTo(os);
    }

    public <T> T deserialize(String type, ByteData data, boolean cacheElements) {
        return deserialize(type, data, 0, cacheElements);
    }

    @SuppressWarnings("unchecked")
    public <T> T deserialize(String type, ByteData data, int position, boolean cacheElements) {
        FlatBlobDeserializationRecord rec = ((FlatBlobFrameworkDeserializer)frameworkDeserializer).getDeserializationRecord(type);
        rec.setCacheElements(cacheElements);
        rec.setByteData(data);
        rec.position(position);

        return (T) getSerializer(type).deserialize(rec);
    }

    public <T> T getCached(String type, int ordinal) {
        FlatBlobTypeCache<T> typeCache = ((FlatBlobFrameworkDeserializer)frameworkDeserializer).getTypeCache(type);
        return typeCache.get(ordinal);
    }

    <T> FlatBlobTypeCache<T> getTypeCache(String type) {
        return ((FlatBlobFrameworkDeserializer)frameworkDeserializer).getTypeCache(type);
    }

}
