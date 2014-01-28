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

public class FlatBlobTypeCache<T> {

    public static int cachedCount = 0;
    public static int uncachedCount = 0;

    private final String name;
    private final ConcurrentHashMap<Integer, T> references;

    public FlatBlobTypeCache(String name) {
        this.name = name;
        this.references = new ConcurrentHashMap<Integer, T>();
    }

    public String getName() {
        return name;
    }

    public void set(int ordinal, T obj) {
        if(ordinal >= 0)
            references.put(Integer.valueOf(ordinal), obj);
    }

    public T get(int ordinal) {
        if(ordinal < 0)
            return null;
        T cached = references.get(Integer.valueOf(ordinal));
        if(cached != null) {
            cachedCount++;
        } else {
            uncachedCount++;
        }
        return cached;
    }
}
