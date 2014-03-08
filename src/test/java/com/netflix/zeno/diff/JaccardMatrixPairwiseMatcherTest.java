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

import com.netflix.zeno.genericobject.GenericObject.Field;
import com.netflix.zeno.genericobject.JaccardMatrixPairwiseMatcher;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JaccardMatrixPairwiseMatcherTest {

    private DiffSerializationFramework diffFramework = null;
    private JaccardMatrixPairwiseMatcher matcher = null;

    @Before
    public void setUp() {
        List<Field> objects1 = new ArrayList<Field>();
        List<Field> objects2 = new ArrayList<Field>();
        List<DiffRecord> recs1 = new ArrayList<DiffRecord>();
        List<DiffRecord> recs2 = new ArrayList<DiffRecord>();

        diffFramework = new DiffSerializationFramework(new SerializerFactory() {
            public NFTypeSerializer<?>[] createSerializers() {
                return new NFTypeSerializer<?>[] { new ASerializer() };
            }
        });

        addObject(new TypeB(1, 1, 1), "TypeB", objects1, recs1);
        addObject(new TypeB(2, 2, 2), "TypeB", objects1, recs1);
        addObject(new TypeB(3, 3, 3), "TypeB", objects1, recs1);

        addObject(new TypeB(3, 3, 3), "TypeB", objects2, recs2);
        addObject(new TypeB(4, 5, 6), "TypeB", objects2, recs2);
        addObject(new TypeB(2, 4, 4), "TypeB", objects2, recs2);
        addObject(new TypeB(1, 1, 2), "TypeB", objects2, recs2);

        matcher = new JaccardMatrixPairwiseMatcher(objects1, recs1, objects2, recs2);
    }

    private void addObject(Object obj, String serializerName, List<Field> objs, List<DiffRecord> recs) {
        NFTypeSerializer<Object> serializer = (NFTypeSerializer<Object>) diffFramework.getSerializer(serializerName);
        DiffRecord rec = new DiffRecord();
        rec.setSchema(serializer.getFastBlobSchema());
        rec.setTopLevelSerializerName(serializerName);
        serializer.serialize(obj, rec);

        objs.add(new Field("obj", obj));
        recs.add(rec);
    }


    @Test
    public void matchesPairs() {
        Assert.assertTrue(matcher.nextPair());
        Assert.assertEquals(new TypeB(3, 3, 3), matcher.getX().getValue());
        Assert.assertEquals(new TypeB(3, 3, 3), matcher.getY().getValue());
        Assert.assertTrue(matcher.nextPair());
        Assert.assertEquals(new TypeB(1, 1, 1), matcher.getX().getValue());
        Assert.assertEquals(new TypeB(1, 1, 2), matcher.getY().getValue());
        Assert.assertTrue(matcher.nextPair());
        Assert.assertEquals(new TypeB(2, 2, 2), matcher.getX().getValue());
        Assert.assertEquals(new TypeB(2, 4, 4), matcher.getY().getValue());
        Assert.assertTrue(matcher.nextPair());
        Assert.assertNull(matcher.getX());
        Assert.assertEquals(new TypeB(4, 5, 6), matcher.getY().getValue());
        Assert.assertFalse(matcher.nextPair());
    }


}
