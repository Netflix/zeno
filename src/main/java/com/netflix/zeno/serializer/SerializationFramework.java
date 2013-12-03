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
package com.netflix.zeno.serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of the abstract class SerializationFramework will describe some operation
 * that can be performed on data in a semantically and structurally agnostic way.<p/>
 *
 * For a given POJO model, a set of {@link NFTypeSerializer} implementations must be created to describe their
 * structure.  A set of serializers, identified by a SerializerFactory, is passed into the constructor
 * of this Object.<p/>
 *
 * The SerializationFramework implementation will traverse this hierarchy of serializers in order to
 * perform its operation.<p/>
 *
 * Example implementations of SerializationFramework are:<p/>
 *
 * HashSerializationFramework:     Calculates an MD5 sum for POJOs conforming to the NFTypeSerializers.<br/>
 * JsonSerializationFramework:     Translates POJOs to json (and vice versa)<br/>
 * FastBlobSerializationFramework: Creates binary dumps of data states represented by POJOs<br/>
 *
 * By maintaining a clean separation of data semantics / structure and the operations which can be performed
 * on the data, we avoid multiplying the work required to maintain our data model by the number of operations
 * we can perform.<p/>
 *
 * Supporting new functionality is easy; we can define a new SerializationFramework.  Adding to the data model
 * is easy; we simply create / modify {@link NFTypeSerializer}s
 *
 * For details about how to create new SerializationFramework implementations, check out the section 
 * <a href="https://github.com/Netflix/zeno/wiki/Creating-new-operations">creating new operations</a> in the 
 * Zeno documentation.
 * 
 * @author dkoszewnik
 *
 */
public abstract class SerializationFramework {

    protected FrameworkSerializer<?> frameworkSerializer;
    protected FrameworkDeserializer<?> frameworkDeserializer;
    protected NFTypeSerializer<?>[] topLevelSerializers;
    private final Map<String, NFTypeSerializer<?>> serializers = new HashMap<String, NFTypeSerializer<?>>();

    /// The NFTypeSerializers, ordered such that all dependencies come *before* their dependents
    private final List<NFTypeSerializer<?>> orderedSerializers = new ArrayList<NFTypeSerializer<?>>();

    public SerializationFramework(SerializerFactory serializerFactory) {
        this.topLevelSerializers = serializerFactory.createSerializers();
        addSerializerTree(topLevelSerializers);
    }

    private void addSerializerTree(NFTypeSerializer<?>... topLevelSerializers) {
        Set<String> alreadyAddedSerializers = new HashSet<String>();
        for(NFTypeSerializer<?> serializer : topLevelSerializers) {
            addSerializer(serializer, alreadyAddedSerializers, true);
        }
    }

    private void addSerializer(NFTypeSerializer<?> serializer, Set<String>alreadyAddedSerializers, boolean topLevelSerializer) {
        if(firstTimeSeeingSerializer(serializer, alreadyAddedSerializers)) {
            for(NFTypeSerializer<?> subSerializer : serializer.requiredSubSerializers()) {
                addSerializer(subSerializer, alreadyAddedSerializers, false);
            }

            serializer.setSerializationFramework(this);
            serializers.put(serializer.getName(), serializer);
            orderedSerializers.add(serializer);
        }
    }

    private boolean firstTimeSeeingSerializer(NFTypeSerializer<?> serializer, Set<String>alreadyAddedSerializers) {
        if(!alreadyAddedSerializers.contains(serializer.getName())) {
            alreadyAddedSerializers.add(serializer.getName());
            return true;
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
    public FrameworkSerializer getFrameworkSerializer() {
        return frameworkSerializer;
    }

    @SuppressWarnings("rawtypes")
    public FrameworkDeserializer getFrameworkDeserializer() {
        return frameworkDeserializer;
    }

    /**
     * @return the NFTypeSerializers ordered such that dependencies come *before* their dependents.
     */
    public List<NFTypeSerializer<?>> getOrderedSerializers() {
        return orderedSerializers;
    }

    @SuppressWarnings("unchecked")
    public <T> NFTypeSerializer<T> getSerializer(String typeName) {
        return (NFTypeSerializer<T>) serializers.get(typeName);
    }

    public NFTypeSerializer<?>[] getTopLevelSerializers() {
        return topLevelSerializers;
    }

}
