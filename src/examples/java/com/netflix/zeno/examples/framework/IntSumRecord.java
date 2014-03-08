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
package com.netflix.zeno.examples.framework;

import com.netflix.zeno.serializer.NFSerializationRecord;

/**
 * When implementing a SerializationFramework, we need to create some kind of "serialization record".<p/>
 *
 * The role of the serialization record is to maintain some state while traversing an object instance.<p/>
 *
 * In our contrived example, this means we will have to keep track of a sum.<p/>
 *
 * A "serialization record" must implement NFSerializationRecord.  This interface defines no methods and is only
 * intended to indicate the role of the class.
 *
 * @author dkoszewnik
 *
 */
public class IntSumRecord extends NFSerializationRecord {

    private int sum;

    public void addValue(int value) {
        sum += value;
    }

    public int getSum() {
        return sum;
    }

}
