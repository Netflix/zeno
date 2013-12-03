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

public class TypeA {

    private final int val1;
    private final int val2;

    public TypeA(int val1, int val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public int getVal1() {
        return val1;
    }

    public int getVal2() {
        return val2;
    }

    @Override
    public int hashCode() {
        return 31 * val1 + 13 * val2;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TypeA other = (TypeA) obj;
        if (val1 != other.val1)
            return false;
        if (val2 != other.val2)
            return false;
        return true;
    }

    public String toString() {
        return "<" + val1 + "," + val2 + ">";

    }

}
