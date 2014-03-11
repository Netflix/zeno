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
package com.netflix.zeno.fastblob;

import com.netflix.zeno.fastblob.record.VarInt;
import com.netflix.zeno.fastblob.state.ByteArrayOrdinalMap;
import com.netflix.zeno.fastblob.state.FastBlobTypeDeserializationState;
import com.netflix.zeno.fastblob.state.FastBlobTypeSerializationState;
import com.netflix.zeno.fastblob.state.TypeDeserializationStateListener;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.util.SimultaneousExecutor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * This is the SerializationFramework for the second-generation blob.<p/>
 *
 * The blob is a serialized representation of all data conforming to an object model (defined by a {@link SerializerFactory})
 * in a single binary file.<p/>
 *
 * This class is the main interface for both serialization, as well as deserialization, of FastBlob data.  For detailed
 * usage of the FastBlobStateEngine, please see <a href="https://github.com/Netflix/zeno/wiki">the Zeno documentation</a><p/>
 *
 * This class holds references to the "TypeSerializationStates", which are responsible for assigning and maintaining the mappings between
 * serialized representations of Objects and "ordinals" (@see {@link ByteArrayOrdinalMap}).<p/>
 *
 * This class also holds references to "TypeDeserializationStates", which are responsible for assigning and maintaining the reverse mapping
 * between ordinals and Objects.<p/>
 *
 * This class also maintains an ordered list of SerializationStateConfiguration objects, which define the specifications for object
 * membership within images.<p/>
 *
 * <a href="https://docs.google.com/presentation/d/1GOIsGUpPVpRX_rY2GzHVCJ2lmf2eb42N3iqlKKcL5wc/edit?usp=sharing">Original presentation for the blob:</a><p/>
 *
 * This class has a lifecycle during which it alternates between two states:<p/>
 * <ol>
 * <li>Safe to add objects, but not safe to write contained objects to a stream.</li>
 * <li>Not safe to add objects, but safe to write contained objects to a stream.</li>
 * </ol>
 *
 * Initially the object will be in state (1).<br/>
 * From state (1), if prepareForWrite() is called, it will be transitioned to state (2).<br/>
 * From state (2), calling prepareForNextCycle() will transition back to state (1).<br/>
 *
 * @see https://github.com/Netflix/zeno/wiki
 *
 * @author dkoszewnik
 *
 */
public class FastBlobStateEngine extends FastBlobSerializationFramework {

    /// all serialization and deserialization states, keyed by their unique names
    private final Map<String, FastBlobTypeSerializationState<?>> serializationTypeStates;
    private final Map<String, FastBlobTypeDeserializationState<?>> deserializationTypeStates;

    /// The serialization states, ordered such that all dependencies come *before* their dependents
    public final List<FastBlobTypeSerializationState<?>> orderedSerializationStates;

    private final int numberOfConfigurations;

    private String latestVersion;
    private Map<String,String> headerTags = new HashMap<String, String>();

    private int maxSingleObjectLength;

    private final boolean[] addToAllImagesFlags;

    public FastBlobStateEngine(SerializerFactory factory) {
        this(factory, 1);
    }

    public FastBlobStateEngine(SerializerFactory factory, int numberOfConfigurations) {
        super(factory);
        this.frameworkSerializer = new FastBlobFrameworkSerializer(this);
        this.frameworkDeserializer = new FastBlobFrameworkDeserializer(this);

        this.serializationTypeStates = new HashMap<String, FastBlobTypeSerializationState<?>>();
        this.deserializationTypeStates = new HashMap<String, FastBlobTypeDeserializationState<?>>();
        this.orderedSerializationStates = new ArrayList<FastBlobTypeSerializationState<?>>();

        this.numberOfConfigurations = numberOfConfigurations;

        addToAllImagesFlags = new boolean[numberOfConfigurations];
        Arrays.fill(addToAllImagesFlags, true);

        createSerializationStates();
    }

    protected void createSerializationStates() {
        for(NFTypeSerializer<?> serializer : getOrderedSerializers()) {
            createSerializationState(serializer);
        }
    }

    private <T> void createSerializationState(NFTypeSerializer<T> serializer) {
        FastBlobTypeSerializationState<T> serializationState = new FastBlobTypeSerializationState<T>(serializer, numberOfConfigurations);
        serializationTypeStates.put(serializer.getName(), serializationState);
        orderedSerializationStates.add(serializationState);
        deserializationTypeStates.put(serializer.getName(), new FastBlobTypeDeserializationState<T>(serializer));
    }


    /**
     * Returns the images which can be generated from this FastBlobStateEngine.  The ordering here is important.<p/>
     *
     * The index at which a SerializationStateConfiguration is returned must be used to specify whether or not
     * each object added to the FastBlobStateEngine is included in that image (see add()).
     *
     */
    public int getNumberOfConfigurations() {
        return numberOfConfigurations;
    }


    /**
     * Add an object to this state engine.  This object will be added to all images.
     */
    public void add(String type, Object obj) {
        add(type, obj, addToAllImagesFlags);
    }

    /**
     * Add an object to this state engine.  The images to which this object should be added are specified with the addToImageFlags[] array of booleans.<p/>
     *
     * For example, if the FastBlobStateEngine can produce 3 images, getImageConfigurations() will return a List of size 3.<p/>
     *
     * If an object added to this state engine should be contained in the images at index 1, but not at index 0 and 2,
     * then the boolean[] passed into this method should be {false, true, false}.
     *
     */
    public void add(String type, Object obj, boolean addToImageFlags[]) {
        FastBlobTypeSerializationState<Object> typeSerializationState = getTypeSerializationState(type);
        if(typeSerializationState == null) {
            throw new RuntimeException("Unable to find type.  Ensure there exists an NFTypeSerializer with the name: "  + type);
        }
        typeSerializationState.add(obj, addToImageFlags);
    }

    /**
     * Add a {@link TypeDeserializationStateListener} to the specified type
     */
    public <T> void setTypeDeserializationStateListener(String type, TypeDeserializationStateListener<T> listener) {
        FastBlobTypeDeserializationState<T> typeState = getTypeDeserializationState(type);
        if(typeState == null) {
            throw new RuntimeException("Unable to find type.  Ensure there exists an NFTypeSerializer with the name: "  + type);
        }

        typeState.setListener(listener);
    }

    /**
     * @return the FastBlobSerializationStates in the order in which they should appear in the FastBlob stream.<p/>
     *
     * See https://docs.google.com/presentation/d/1G98w4W0Nb8MzBvglVCwd698aUli4NOFEin60lGZeJos/edit?usp=sharing for a
     * detailed explanation of why this ordering exists and how it is derived.
     */
    public List<FastBlobTypeSerializationState<?>> getOrderedSerializationStates() {
        return orderedSerializationStates;
    }

    /**
     * @return The unmodifiableSet of names
     */
    public Set<String> getSerializerNames() {
        return Collections.unmodifiableSet(serializationTypeStates.keySet());
    }

    @SuppressWarnings("unchecked")
    public <T> FastBlobTypeSerializationState<T> getTypeSerializationState(String name) {
        return (FastBlobTypeSerializationState<T>) serializationTypeStates.get(name);
    }

    @SuppressWarnings("unchecked")
    public <T> FastBlobTypeDeserializationState<T> getTypeDeserializationState(String name) {
        return (FastBlobTypeDeserializationState<T>) deserializationTypeStates.get(name);
    }

    /**
     * Create a lookup array (from ordinal to serialized byte data) for each FastBlobSerializationState.<p/>
     *
     * Determines and remembers the maximum single object length, in bytes.
     */
    public void prepareForWrite() {
        maxSingleObjectLength = 0;

        for(FastBlobTypeSerializationState<?> state : orderedSerializationStates) {
            int stateMaxLength = state.prepareForWrite();
            if(stateMaxLength > maxSingleObjectLength) {
                maxSingleObjectLength = stateMaxLength;
            }
        }
    }

    public void prepareForNextCycle() {
        for(FastBlobTypeSerializationState<?> state : orderedSerializationStates) {
            state.prepareForNextCycle();
        }
    }

    public int getMaxSingleObjectLength() {
        return maxSingleObjectLength;
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

    /// arbitrary version number.  Change this when incompatible modifications are made to the state engine
    /// serialization format.
    private final int STATE_ENGINE_SERIALIZATION_FORMAT_VERSION = 999996;

    /**
     *  Serialize a previous serialization state from the stream.  The deserialized state engine will be in exactly the same state as the serialized state engine.
     */
    public void serializeTo(OutputStream os) throws IOException {
        DataOutputStream dos = new DataOutputStream(os);

        dos.writeInt(STATE_ENGINE_SERIALIZATION_FORMAT_VERSION);

        dos.writeUTF(latestVersion);

        dos.writeShort(headerTags.size());
        for(Map.Entry<String,String> headerTag : headerTags.entrySet()) {
            dos.writeUTF(headerTag.getKey());
            dos.writeUTF(headerTag.getValue());
        }

        VarInt.writeVInt(dos, numberOfConfigurations);

        VarInt.writeVInt(dos, orderedSerializationStates.size());

        for(FastBlobTypeSerializationState<?> typeState : orderedSerializationStates) {
            dos.writeUTF(typeState.getSchema().getName());
            typeState.serializeTo(dos);
        }
    }

    /**
     * Reinstantiate a StateEngine from the stream.
     */
    public void deserializeFrom(InputStream is) throws IOException {
        DataInputStream dis = new DataInputStream(is);

        if(dis.readInt() != STATE_ENGINE_SERIALIZATION_FORMAT_VERSION) {
            throw new RuntimeException("Refusing to reinstantiate FastBlobStateEngine due to serialized version mismatch.");
        }

        latestVersion = dis.readUTF();
        int numHeaderTagEntries = dis.readShort();
        headerTags.clear();
        headerTags = new HashMap<String, String>();
        for(int i=0;i<numHeaderTagEntries;i++) {
            headerTags.put(dis.readUTF(), dis.readUTF());
        }

        int numConfigs = VarInt.readVInt(dis);

        int numStates = VarInt.readVInt(dis);

        for(int i=0;i<numStates;i++) {
            String typeName = dis.readUTF();
            FastBlobTypeSerializationState<?> typeState = serializationTypeStates.get(typeName);

            if(typeState != null) {
                typeState.deserializeFrom(dis, numConfigs);
            } else {
                FastBlobTypeSerializationState.discardSerializedTypeSerializationState(dis, numConfigs);
            }
        }
    }

    /*
     * Copy all the serialization states to provided state engine
     */
    public void copyTo(FastBlobStateEngine otherStateEngine) {
        copyTo(otherStateEngine, Collections.<String> emptyList());
    }

    /*
     * Copy serialization states whose serializer's name doesn't match the ones provided in the ignore list
     */
    public void copyTo(FastBlobStateEngine otherStateEngine, Collection<String> topLevelSerializersToIgnore) {
        fillDeserializationStatesFromSerializedData();

        SimultaneousExecutor executor = new SimultaneousExecutor(4.0d);

        List<String> topLevelSerializersToCopy = new ArrayList<String>();
        for(NFTypeSerializer<?> serializer : getTopLevelSerializers()) {
            String serializerName = serializer.getName();
            if(!topLevelSerializersToIgnore.contains(serializerName)) {
                topLevelSerializersToCopy.add(serializer.getName());
            }
        }
        
        CountDownLatch latch = new CountDownLatch(executor.getMaximumPoolSize() * topLevelSerializersToCopy.size());

        for(String serializerizerName : topLevelSerializersToCopy) {
            executor.submit(getFillSerializationStateRunnable(otherStateEngine, serializerizerName, executor, latch));
        }

        try {
            latch.await();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
        executor.shutdown();
    }

    private Runnable getFillSerializationStateRunnable(final FastBlobStateEngine otherStateEngine,
            final String serializerName, final SimultaneousExecutor executor, final CountDownLatch latch) {
        return new Runnable() {
            @Override
            public void run() {
                fillSerializationState(otherStateEngine, serializerName, executor, latch);
            }
        };
    }

    private void fillSerializationState(FastBlobStateEngine otherStateEngine,
            String serializerName, final SimultaneousExecutor executor, CountDownLatch latch) {
        int threadsSize = executor.getMaximumPoolSize();
        for(int i=0;i<threadsSize;i++) {
            executor.submit(getFillSerializationStatesRunnable(otherStateEngine, serializerName, threadsSize, latch, i));
        }
    }

    private Runnable getFillSerializationStatesRunnable(final FastBlobStateEngine otherStateEngine, 
            final String serializerName, final int numThreads, final CountDownLatch latch, final int threadNumber) {
        return new Runnable() {
            @Override
            public void run() {
                copyObjects(otherStateEngine, serializerName, numThreads, threadNumber);
                latch.countDown();
            }
        };
    }


    /**
     * Explode the data from the serialization states into the deserialization states.<p/>
     *
     * This is used during FastBlobStateEngine combination.<p/>
     *
     * @param otherStateEngineSystem.out.println("#######Copied Serialization states in " + (System.currentTimeMillis() - startTime) + "ms");
     */
    private void fillDeserializationStatesFromSerializedData() {
        for(FastBlobTypeSerializationState<?> serializationState : getOrderedSerializationStates()) {
            serializationState.fillDeserializationState(getTypeDeserializationState(serializationState.getSchema().getName()));
        }
    }

    public void prepareForDoubleSnapshotRefresh() {
        this.frameworkSerializer = new FastBlobHeapFriendlyClientFrameworkSerializer(this);
    }

    public void cleanUpAfterDoubleSnapshotRefresh() {
        for(FastBlobTypeDeserializationState<?> state : deserializationTypeStates.values()) {
            state.clearIdentityOrdinalMap();
        }
    }

    private void copyObjects(final FastBlobStateEngine otherStateEngine, final String serializerName,
            final int numThreads, final int threadNumber) {
        FastBlobTypeDeserializationState<?> typeDeserializationState = getTypeDeserializationState(serializerName);       
        int maxOrdinal = typeDeserializationState.maxOrdinal() + 1;
        if(maxOrdinal < threadNumber) {
            return;
        }
        
        FastBlobTypeSerializationState<?> typeSerializationState = getTypeSerializationState(serializerName);
        boolean imageMembershipsFlags[] = new boolean[numberOfConfigurations];

        for(int i=threadNumber;i<maxOrdinal;i+=numThreads) {
            Object obj = typeDeserializationState.get(i);
            if(obj != null) {
                for(int imageIndex=0;imageIndex<numberOfConfigurations;imageIndex++) {
                    imageMembershipsFlags[imageIndex] = typeSerializationState.getImageMembershipBitSet(imageIndex).get(i);
                }
                otherStateEngine.add(typeSerializationState.getSchema().getName(), obj, imageMembershipsFlags);
            }
        }
    }

}
