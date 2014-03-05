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

import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * An example framework which sums all of the integers contained in an object instance (anywhere in the
 * hierarchy defined by the data model). <p/>
 *
 * Follow along in the documentation page <a href="https://github.com/Netflix/zeno/wiki/Creating-new-operations">creating new operations</a>
 *
 * @author dkoszewnik
 *
 */
public class IntSumFramework extends SerializationFramework {

    public IntSumFramework(SerializerFactory serializerFactory) {
        super(serializerFactory);
        this.frameworkSerializer = new IntSumFrameworkSerializer(this);
    }

    public <T> int getSum(String type, T obj) {
        IntSumRecord record = new IntSumRecord();
        getSum(type, obj, record);
        return record.getSum();
    }

    <T> void getSum(String type, T obj, IntSumRecord record) {
        getSerializer(type).serialize(obj, record);
    }

}
