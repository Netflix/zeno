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
package com.netflix.zeno.util;


import com.netflix.zeno.util.collections.MinimizedUnmodifiableCollections;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
*
* Unwraps collections, which have been wrapped with Unmodifiable wrappers.<p/>
*
* If comparing two Collections for "sameness" (==), it is useful to compare the "unwrapped" instances
* of these Objects, to avoid false negatives when comparing a wrapped vs an unwrapped instance.
*
* @author dkoszewnik
*
*/
public class CollectionUnwrapper {

    private static final Class<?> unmodifiableCollectionClass = getInnerClass(Collections.class, "UnmodifiableCollection");
    private static final Field unmodifiableCollectionField = getField(unmodifiableCollectionClass, "c");

    private static final Class<?> unmodifiableMapClass = getInnerClass(Collections.class, "UnmodifiableMap");
    private static final Field unmodifiableMapField = getField(unmodifiableMapClass, "m");

    private static Class<?> getInnerClass(Class<?> fromClass, String className) {
        for(Class<?> c : fromClass.getDeclaredClasses()) {
            if(c.getSimpleName().equals(className)) {
                return c;
            }
        }
        return null;
    }

    private static Field getField(Class<?> fromClass, String fieldName) {
        try {
            Field f = fromClass.getDeclaredField(fieldName);
            f.setAccessible(true);
            return f;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T unwrap(Object obj) {
        if(obj instanceof Collection) {
            if(((Collection<?>)obj).isEmpty()) {
                if(obj instanceof List)
                    return (T) Collections.EMPTY_LIST;
                if(obj instanceof Set)
                    return (T) Collections.EMPTY_SET;
            }

            if(unmodifiableCollectionClass.isInstance(obj)) {
                return unwrap(obj, unmodifiableCollectionClass, unmodifiableCollectionField);
            }
        } else if(obj instanceof Map) {
            if(((Map<?,?>)obj).isEmpty()) {
                if(obj instanceof SortedMap)
                    return (T) MinimizedUnmodifiableCollections.EMPTY_SORTED_MAP;
                return (T) Collections.EMPTY_MAP;
            }

            if(unmodifiableMapClass.isInstance(obj)) {
                return unwrap(obj, unmodifiableMapClass, unmodifiableMapField);
            }
        }

        return (T) obj;
    }

    @SuppressWarnings("unchecked")
    private static <T> T unwrap(Object obj, Class<?> clazz, Field field) {
        try {
            do {
                obj = field.get(obj);
            } while(clazz.isInstance(obj));
            return (T) obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

