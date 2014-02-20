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
     * Called once each time an instance is removed from the TypeDeserializationState<p/>
     *
     * @deprecated use removedObject(T obj, int ordinal) instead.
     */
    @Deprecated
    public void removedObject(T obj) { }

    /**
     * Called once each time an instance is removed from the TypeDeserializationState.<p/>
     *
     * Please note that in the case of a double snapshot load, all object ordinals are shuffled.
     * In this case, the "obj" parameter may not currently be assigned to the provided ordinal, and
     * addedObject may have been called with a different object at the same ordinal.
     */
    public abstract void removedObject(T obj, int ordinal);


    /**
     * Called once each time an instance's ordinal is reassigned.  This will happen in the case of a double snapshot reload.<p/>
     *
     * This is called once for every object which is copied from the previous state, whether or not the ordinal has changed.  In some cases,
     * oldOrdinal and newOrdinal may be the same value.
     */
    public abstract void reassignedObject(T obj, int oldOrdinal, int newOrdinal);

    /**
     * Called once each time an instance is added to the TypeSerializationState
     *
     * @deprecated use addedObject(T obj, int ordinal) instead.
     */
    @Deprecated
    public void addedObject(T obj) { }

    /**
     * Called once each time an object instance is added to the TypeSerializationState
     */
    public abstract void addedObject(T obj, int ordinal);

    private static final TypeDeserializationStateListener<Object> NOOP_CALLBACK =
            new TypeDeserializationStateListener<Object>() {
                @Override
                public void removedObject(Object obj, int ordinal) { }

                @Override
                public void addedObject(Object obj, int ordinal) { }

                @Override
                public void reassignedObject(Object obj, int oldOrdinal, int newOrdinal) { }
            };

    /**
     * @return a callback which does nothing with modification events
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeDeserializationStateListener<T> noopCallback() {
        return (TypeDeserializationStateListener<T>) NOOP_CALLBACK;
    }

}
