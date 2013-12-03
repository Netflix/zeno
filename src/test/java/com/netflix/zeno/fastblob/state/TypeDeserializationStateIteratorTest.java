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
package com.netflix.zeno.fastblob.state;


import com.netflix.zeno.fastblob.state.TypeDeserializationStateIterator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;


public class TypeDeserializationStateIteratorTest {

    @Test
    public void skipsOverNullValues() {
        List<Integer> list = new ArrayList<Integer>();

        list.add(null);
        list.add(1);
        list.add(2);
        list.add(null);
        list.add(4);
        list.add(null);

        TypeDeserializationStateIterator<Integer> iter = new TypeDeserializationStateIterator<Integer>(list);

        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(iter.next(), Integer.valueOf(1));
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(iter.next(), Integer.valueOf(2));
        Assert.assertEquals(iter.next(), Integer.valueOf(4));
        Assert.assertFalse(iter.hasNext());
    }

}
