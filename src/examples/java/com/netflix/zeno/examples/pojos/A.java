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


public class A {

    private final List<B> bList;
    private final C cValue;
    private final int intValue;

    public A(List<B> bList, C cValue, int intValue) {
        this.bList = bList;
        this.cValue = cValue;
        this.intValue = intValue;
    }

    public List<B> getBList() {
        return bList;
    }

    public C getCValue() {
        return cValue;
    }

    public int getIntValue() {
        return intValue;
    }

}
