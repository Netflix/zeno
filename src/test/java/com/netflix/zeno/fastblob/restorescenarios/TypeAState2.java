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
package com.netflix.zeno.fastblob.restorescenarios;

public class TypeAState2 {

    private final int a1;
    private final TypeC c;

    public TypeAState2(int a1, TypeC c) {
        this.a1 = a1;
        this.c = c;
    }

    public int getA1() {
        return a1;
    }

    public TypeC getC() {
        return c;
    }
}
