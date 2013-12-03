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
package com.netflix.zeno.testpojos;

import java.util.List;
import java.util.Map;

public class TypeC {

    private final Map<String, TypeA> typeAMap;
    private final List<TypeB> typeBs;

    public TypeC(Map<String, TypeA> typeAMap, List<TypeB> typeBs) {
        this.typeAMap = typeAMap;
        this.typeBs = typeBs;
    }

    public Map<String, TypeA> getTypeAMap() {
        return typeAMap;
    }

    public List<TypeB> getTypeBs() {
        return typeBs;
    }

    @Override
    public int hashCode() {
        return typeAMap.hashCode() ^ typeBs.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeC other = (TypeC) obj;
        if (typeAMap == null) {
            if (other.typeAMap != null)
                return false;
        } else if (!typeAMap.equals(other.typeAMap))
            return false;
        if (typeBs == null) {
            if (other.typeBs != null)
                return false;
        } else if (!typeBs.equals(other.typeBs))
            return false;
        return true;
    }



}
