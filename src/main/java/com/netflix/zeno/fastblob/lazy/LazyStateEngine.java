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

import com.netflix.zeno.fastblob.lazy.hollow.HollowObject;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * LazyStateEngine is currently an experiment, the goal of this is to keep the FastBlob data in memory,
 * then read on demand from memory.
 *
 * This keeps the memory footprint low, but is more scalable than over-the-network calls and much
 * more performant than reaching to disk (e.g. for a flat blob).
 *
 * @author dkoszewnik
 *
 */
public class LazyStateEngine extends SerializationFramework {

    private final Map<String, LazyTypeDataState<?>> typeDataStates;

    private String latestVersion;
    private final Map<String,String> headerTags = new HashMap<String, String>();


    public LazyStateEngine(SerializerFactory serializerFactory) {
        super(serializerFactory);
        this.typeDataStates = new HashMap<String, LazyTypeDataState<?>>();

        createTypeDeserializationStates();
    }

    public HollowObject getHollowObject(String type, int ordinal) {
        return typeDataStates.get(type).get(ordinal);
    }

    public boolean positionHollowObject(String type, int ordinal, HollowObject objectToPosition) {
        return typeDataStates.get(type).position(objectToPosition, ordinal);
    }

    private void createTypeDeserializationStates() {
        for(NFTypeSerializer<?>serializer : getOrderedSerializers()) {
            typeDataStates.put(serializer.getName(), createTypeDeserializationState(serializer));
        }
    }

    private <T> LazyTypeDataState<T> createTypeDeserializationState(NFTypeSerializer<T>serializer) {
        return new LazyTypeDataState<T>(serializer, this);
    }

    @SuppressWarnings("unchecked")
    public <T> LazyTypeDataState<T> getTypeDataState(String name) {
        return (LazyTypeDataState<T>) typeDataStates.get(name);
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
