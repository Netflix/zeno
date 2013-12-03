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

public class TypeB {

    private final int val1;
    private final int val2;
    private final int val3;

    public TypeB(int val1, int val2, int val3) {
        this.val1 = val1;
        this.val2 = val2;
        this.val3 = val3;
    }

    public int getVal1() {
        return val1;
    }

    public int getVal2() {
        return val2;
    }

    public int getVal3() {
        return val3;
    }

    public int hashCode() {
        return val1 + 31 * val2 + 91 * val3;
    }

    public boolean equals(Object another) {
        if(another instanceof TypeB) {
            TypeB otherB = (TypeB) another;

            return otherB.val1 == val1 && otherB.val2 == val2 && otherB.val3 == val3;
        }
        return false;
    }

    public String toString() {
        return "TypeB { " + val1 + "," + val2 + "," + val3 + " }";
    }
}
