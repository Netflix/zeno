package com.netflix.zeno.fastblob.lazy;

import com.netflix.zeno.fastblob.FastBlobFrameworkDeserializer;
import com.netflix.zeno.fastblob.FastBlobSerializationFramework;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

import java.util.HashMap;
import java.util.Map;

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
