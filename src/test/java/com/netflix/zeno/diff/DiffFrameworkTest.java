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

import com.netflix.zeno.diff.TypeDiff.ObjectDiffScore;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;
import com.netflix.zeno.testpojos.TypeA;
import com.netflix.zeno.testpojos.TypeASerializer;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class DiffFrameworkTest {

    DiffSerializationFramework framework;

    @Before
    public void setUp() {
        framework = new DiffSerializationFramework(new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new TypeASerializer() };
            }
        });
    }

    @Test
    public void test() {
        List<TypeA> list1 = new ArrayList<TypeA>();
        List<TypeA> list2 = new ArrayList<TypeA>();

        list1.add(new TypeA(1, 2));
        list2.add(new TypeA(1, 2));

        list1.add(new TypeA(2, 3));
        list2.add(new TypeA(2, 4));

        list1.add(new TypeA(3, 4));
        list2.add(new TypeA(4, 6));

        list1.add(new TypeA(5, 7));
        list2.add(new TypeA(5, 8));

        list1.add(new TypeA(6, 9));
        list2.add(new TypeA(6, 10));

        list1.add(new TypeA(7, 11));
        list2.add(new TypeA(8, 12));

        TypeDiffInstruction<TypeA> diffInstruction = new TypeDiffInstruction<TypeA>() {
            public String getSerializerName() {
                return "TypeA";
            }

            @Override
            public Object getKey(TypeA object) {
                return Integer.valueOf(object.getVal1());
            }
        };

        TypeDiffOperation<TypeA> diffOperation = new TypeDiffOperation<TypeA>(diffInstruction);

        TypeDiff<TypeA> typeDiff = diffOperation.performDiff(framework, list1, list2, 2);

        Assert.assertEquals(2, typeDiff.getExtraInFrom().size());
        Assert.assertTrue(typeDiff.getExtraInFrom().contains(new TypeA(3, 4)));
        Assert.assertTrue(typeDiff.getExtraInFrom().contains(new TypeA(7, 11)));

        Assert.assertEquals(2, typeDiff.getExtraInTo().size());
        Assert.assertTrue(typeDiff.getExtraInTo().contains(new TypeA(4, 6)));
        Assert.assertTrue(typeDiff.getExtraInTo().contains(new TypeA(8, 12)));

        Assert.assertEquals(3, typeDiff.getDiffObjects().size());
        Assert.assertEquals(2, typeDiff.getDiffObjects().get(0).getScore());
        Assert.assertEquals(2, typeDiff.getDiffObjects().get(1).getScore());
        Assert.assertEquals(2, typeDiff.getDiffObjects().get(2).getScore());

        long fromHashTotal = 0;
        long toHashTotal = 0;
        for (ObjectDiffScore<TypeA> diffScore : typeDiff.getDiffObjects()) {
            TypeA type = diffScore.getFrom();
            Assert.assertTrue(type.equals(new TypeA(2, 3)) || type.equals(new TypeA(5, 7)) || type.equals(new TypeA(6, 9)));

            TypeA toType = diffScore.getTo();
            Assert.assertTrue(toType.equals(new TypeA(2, 4)) || toType.equals(new TypeA(5, 8)) || toType.equals(new TypeA(6, 10)));

            fromHashTotal += type.hashCode();
            toHashTotal += toType.hashCode();
        }
        Assert.assertEquals(650, fromHashTotal);
        Assert.assertEquals(689, toHashTotal);

        Assert.assertEquals(2, typeDiff.getSortedFieldDifferencesDescending().size());
        Assert.assertEquals(0.75, typeDiff.getSortedFieldDifferencesDescending().get(0).getDiffScore().getDiffPercent(), 0.001);
        Assert.assertEquals(0, typeDiff.getSortedFieldDifferencesDescending().get(1).getDiffScore().getDiffPercent(), 0.0);

    }

}
