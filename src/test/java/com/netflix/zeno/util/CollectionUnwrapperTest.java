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

import com.netflix.zeno.util.CollectionUnwrapper;
import com.netflix.zeno.util.collections.MinimizedUnmodifiableCollections;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

public class CollectionUnwrapperTest {

    @Test
    public void unwrapsLists() {
        List<Integer> list = Arrays.asList(1, 2, 3);

        List<Integer> wrapped = Collections.unmodifiableList(list);

        Assert.assertSame(list, CollectionUnwrapper.unwrap(wrapped));
    }

    @Test
    public void unwrapsSets() {
        Set<Integer> set = new HashSet<Integer>();
        set.add(1);

        Set<Integer> wrapped = Collections.unmodifiableSet(set);

        Assert.assertSame(set, CollectionUnwrapper.unwrap(wrapped));
    }

    @Test
    public void unwrapsMaps() {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(1, 2);

        Map<Integer, Integer> wrapped = Collections.unmodifiableMap(map);

        Assert.assertSame(map, CollectionUnwrapper.unwrap(wrapped));
    }

    @Test
    public void returnsNonCollectionsUnchanged() {
        Object o = new Object();

        Assert.assertSame(o, CollectionUnwrapper.unwrap(o));
    }

    @Test
    public void unwrapsMultiplyWrappedCollections() {
        List<Integer> list = Arrays.asList(1, 2, 3);

        List<Integer> wrapped = Collections.unmodifiableList(list);
        List<Integer> doubleWrapped = Collections.unmodifiableList(wrapped);

        Assert.assertSame(list, CollectionUnwrapper.unwrap(doubleWrapped));
    }

    @Test
    public void returnsCanonicalVersionsOfEmptyCollections() {
        Assert.assertSame(Collections.EMPTY_LIST, CollectionUnwrapper.unwrap(new ArrayList<Object>()));
        Assert.assertSame(Collections.EMPTY_SET, CollectionUnwrapper.unwrap(new HashSet<Object>()));
        Assert.assertSame(MinimizedUnmodifiableCollections.EMPTY_SORTED_MAP, CollectionUnwrapper.unwrap(new TreeMap<Object, Object>()));
        Assert.assertSame(Collections.EMPTY_MAP, CollectionUnwrapper.unwrap(new HashMap<Object, Object>()));
    }

}
