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

/**
 * A TypeDeserializationStateListener will listen to modifications which are made to the type state
 * during blob consumption.  These modifications will be communicated as a set of instances which are
 * removed and a set of instances which are added.
 *
 * @author dkoszewnik
 */
public abstract class TypeDeserializationStateListener<T> {

    /**
     * Called once each time an instance is removed from the TypeDeserializationState
     */
    public abstract void removedObject(T obj);

    /**
     * Called once each time an instance is added to the TypeSerializationState
     */
    public abstract void addedObject(T obj);

    private static final TypeDeserializationStateListener<Object> NOOP_CALLBACK =
            new TypeDeserializationStateListener<Object>() {
                @Override
                public void removedObject(Object obj) { }

                @Override
                public void addedObject(Object obj) { }
            };

    /**
     * @return a callback which does nothing with modification events
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeDeserializationStateListener<T> noopCallback() {
        return (TypeDeserializationStateListener<T>) NOOP_CALLBACK;
    }

}
