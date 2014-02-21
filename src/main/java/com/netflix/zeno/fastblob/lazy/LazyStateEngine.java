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

import com.netflix.zeno.fastblob.FastBlobFrameworkDeserializer;
import com.netflix.zeno.fastblob.FastBlobSerializationFramework;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * LazyStateEngine is currently an experiment, the goal of this is to keep the FastBlob data in memory,
 * then deserialize on demand from memory.
 *
 * This keeps the memory footprint low, but is more scalable than over-the-network calls and much
 * more performant than reaching to disk (e.g. for a flat blob).
 *
 * @author dkoszewnik
 *
 */
public class LazyStateEngine extends FastBlobSerializationFramework {

    private final Map<String, LazyTypeDeserializationState<?>> typeDeserializationStates;

    private String latestVersion;
    private final Map<String,String> headerTags = new HashMap<String, String>();


    public LazyStateEngine(SerializerFactory serializerFactory) {
        super(serializerFactory);
        this.frameworkDeserializer = new FastBlobFrameworkDeserializer(this);
        this.typeDeserializationStates = new HashMap<String, LazyTypeDeserializationState<?>>();

        createTypeDeserializationStates();
    }

    private void createTypeDeserializationStates() {
        for(NFTypeSerializer<?>serializer : getOrderedSerializers()) {
            typeDeserializationStates.put(serializer.getName(), createTypeDeserializationState(serializer));
        }
    }

    private <T> LazyTypeDeserializationState<T> createTypeDeserializationState(NFTypeSerializer<T>serializer) {
        return new LazyTypeDeserializationState<T>(serializer);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> LazyTypeDeserializationState<T> getTypeDeserializationState(String name) {
        return (LazyTypeDeserializationState<T>) typeDeserializationStates.get(name);
    }

    public String getLatestVersion() {
        return latestVersion;
    }

    public void setLatestVersion(String latestVersion) {
        this.latestVersion = latestVersion;
    }

    public Map<String,String> getHeaderTags() {
        return headerTags;
    }

    public void addHeaderTags(Map<String,String> headerTags) {
        this.headerTags.putAll(headerTags);
    }

    public void addHeaderTag(String tag, String value) {
        this.headerTags.put(tag, value);
    }

    public String getHeaderTag(String tag) {
        return this.headerTags.get(tag);
    }

}
