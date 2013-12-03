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
package com.netflix.zeno.examples.pojos;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class D {

    private final List<A> list;
    private final Set<B> set;
    private final Map<Integer, C> map;

    public D(List<A> list, Set<B> set, Map<Integer, C> map) {
        this.list = list;
        this.set = set;
        this.map = map;
    }

    public List<A> getList() {
        return list;
    }

    public Set<B> getSet() {
        return set;
    }

    public Map<Integer, C> getMap() {
        return map;
    }

}
