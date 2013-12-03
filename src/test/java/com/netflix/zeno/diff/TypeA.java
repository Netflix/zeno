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
package com.netflix.zeno.diff;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TypeA {

    private final Set<TypeB> typeBs;

    public TypeA(TypeB... typeBs) {
        this.typeBs = new BSet(Arrays.asList(typeBs));
    }

    public TypeA(Set<TypeB> typeBs) {
        this.typeBs = typeBs;
    }

    public Set<TypeB> getTypeBs() {
        return typeBs;
    }

    public int hashCode() {
        return typeBs.hashCode();
    }

    public boolean equals(Object another) {
        if(another instanceof TypeA) {
            TypeA otherA = (TypeA) another;
            return otherA.typeBs.equals(typeBs);
        }
        return false;
    }

    private static class BSet extends AbstractSet<TypeB> {

        List<TypeB> bList = new ArrayList<TypeB>();

        public BSet(List<TypeB> bList) {
            this.bList = bList;
        }

        @Override
        public Iterator<TypeB> iterator() {
            return bList.iterator();
        }

        @Override
        public int size() {
            return bList.size();
        }

    }
}
