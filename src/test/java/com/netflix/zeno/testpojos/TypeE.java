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

public class TypeE {

    private final TypeF typeF;

    public TypeE(TypeF typeF) {
        this.typeF = typeF;
    }

    public TypeF getTypeF() {
        return typeF;
    }

    @Override
    public int hashCode() {
        return 31 * typeF.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeE other = (TypeE) obj;
        if (typeF == null) {
            if (other.typeF != null)
                return false;
        } else if (!typeF.equals(other.typeF))
            return false;
        return true;
    }



}
