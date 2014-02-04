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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FlatBlobTypeCache<T> {

    private final String name;
    private final ConcurrentHashMap<Integer, ObjectIdentityKey> references;
    private final ConcurrentHashMap<ObjectIdentityKey, Integer> ordinalLookup;

    public FlatBlobTypeCache(String name) {
        this.name = name;
        this.references = new ConcurrentHashMap<Integer, ObjectIdentityKey>();
        this.ordinalLookup = new ConcurrentHashMap<ObjectIdentityKey, Integer>();
    }

    public String getName() {
        return name;
    }

    @SuppressWarnings("unchecked")
    public T putIfAbsent(int ordinal, T obj) {
        if(ordinal >= 0) {
            Integer ordinalInteger = Integer.valueOf(ordinal);
            /// create a new key
            ObjectIdentityKey key = new ObjectIdentityKey(obj);
            while(true) {
                /// try to put the key in the references map.
                ObjectIdentityKey existingKey = references.putIfAbsent(ordinalInteger, key);
                if(existingKey == null) {
                    ordinalLookup.put(key, ordinalInteger);
                    return obj;
                }

                /// if unsuccessful, try to increment the references for the key which won the race
                if(existingKey.tryIncrementReferences())
                    return (T) existingKey.getObject();

                /// use the older object in the cache.
                key.setObject(existingKey.getObject());

                /// this will spin, but not acquire the lock, thus preventing starvation
                while(references.get(ordinalInteger) == existingKey);
            }
        }
        return obj;
    }

    public void evict(T obj) {
        ObjectIdentityKey lookupKey = getLookupKey(obj);
        if(lookupKey != null) {
            Integer ordinalInteger = ordinalLookup.get(lookupKey);
            ObjectIdentityKey actualKey = references.get(ordinalInteger);

            if(actualKey.decrementReferences()) {
                ordinalLookup.remove(actualKey);
                references.remove(ordinalInteger);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public T get(int ordinal) {
        if(ordinal < 0)
            return null;
        ObjectIdentityKey identityKey = references.get(Integer.valueOf(ordinal));
        if(identityKey != null && identityKey.tryIncrementReferences())
            return (T) identityKey.getObject();
        return null;
    }

    /// cache lookup keys to reduce object allocation.
    private static ThreadLocal<ObjectIdentityKey> lookupKey = new ThreadLocal<ObjectIdentityKey>();

    private ObjectIdentityKey getLookupKey(Object obj) {
        ObjectIdentityKey key = lookupKey.get();
        if(key == null) {
            key = new ObjectIdentityKey();
            lookupKey.set(key);
        }

        key.setObject(obj);

        return key;
    }

    private static class ObjectIdentityKey {
        private Object obj;
        private final AtomicInteger referenceCount;

        public ObjectIdentityKey() {
            this.referenceCount = new AtomicInteger(0);
        }

        public ObjectIdentityKey(Object obj) {
            this.obj = obj;
            this.referenceCount = new AtomicInteger(1);
        }

        public Object getObject() {
            return obj;
        }

        public void setObject(Object obj) {
            this.obj = obj;
        }

        /**
         * We will only increment references if the number of references does not equal 0.
         *
         * If the number of references reaches 0, then this entry will be scheduled for eviction.
         *
         * @return
         */
        public boolean tryIncrementReferences() {
            while(true) {
                int current = referenceCount.get();
                if(current == 0)
                    return false;
                int next = current + 1;
                if (referenceCount.compareAndSet(current, next))
                    return true;
            }
        }

        /**
         * Decrement references, and return true if the number of references reaches 0.
         *
         * @return
         */
        public boolean decrementReferences() {
            return referenceCount.decrementAndGet() == 0;
        }

        public int hashCode() {
            return System.identityHashCode(obj);
        }

        public boolean equals(Object other) {
            if(other instanceof ObjectIdentityKey) {
                return obj == ((ObjectIdentityKey)other).getObject();
            }
            return false;
        }
    }
}
