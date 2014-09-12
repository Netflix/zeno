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
package com.netflix.zeno.fastblob;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.zeno.testpojos.TypeA;
import com.netflix.zeno.testpojos.TypeB;
import com.netflix.zeno.testpojos.TypeC;
import com.netflix.zeno.testpojos.TypeCSerializer;
import com.netflix.zeno.testpojos.TypeD;

public class SerializerSnapshotTest extends BlobSerializationGenericFrameworkAbstract {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void serializeObjects() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        cache("TypeA", new TypeA(23523452, 2));
        cache("TypeB", new TypeB(3, "four"));

        serializeAndDeserializeSnapshot();

        final List<TypeA> typeAList = getAll("TypeA");
        final List<TypeB> typeBList = getAll("TypeB");

        Assert.assertEquals(1, typeAList.size());
        Assert.assertEquals(1, typeBList.size());
        Assert.assertEquals(23523452, typeAList.get(0).getVal1());
        Assert.assertEquals(2, typeAList.get(0).getVal2());
        Assert.assertEquals(3, typeBList.get(0).getVal1());
        Assert.assertEquals("four", typeBList.get(0).getVal2());
    }


    @Test
    public void deserializeWithoutKnowingAboutSomeObjectTypes() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        cache("TypeA", new TypeA(23523452, 2));
        cache("TypeB", new TypeB(3, "four"));

        final byte data[] = serializeSnapshot();
        serializationState = new FastBlobStateEngine(serializersFactory);
        deserializeSnapshot(data);

        final List<TypeB> typeBList = getAll("TypeB");
        Assert.assertEquals(1, typeBList.size());
        Assert.assertEquals(3, typeBList.get(0).getVal1());
        Assert.assertEquals("four", typeBList.get(0).getVal2());
    }

    @Test
    public void serializeObjectsWithNullPrimitives() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        cache("TypeD", new TypeD(null, null));

        serializeAndDeserializeSnapshot();

        final TypeD deserialized = (TypeD) getAll("TypeD").get(0);

        Assert.assertNull(deserialized.getVal());
    }

    @Test
    public void serializeObjectsWithNullReferences() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        cache("TypeC", new TypeC(null, null));

        serializeAndDeserializeSnapshot();

        final TypeC deserialized = (TypeC) getAll("TypeC").get(0);

        Assert.assertNull(deserialized.getTypeAMap());
        Assert.assertNull(deserialized.getTypeBs());
    }

    @Test
    public void serializeMultipleObjects() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        cache("TypeB", new TypeB(1, "two"));
        cache("TypeB", new TypeB(3, "four"));

        serializeAndDeserializeSnapshot();

        Assert.assertEquals(2, getAll("TypeB").size());

    }

    @Test
    public void deserializedObjectsShareReferences() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        final TypeA theTypeA = new TypeA(1, 2);

        cache("TypeA", theTypeA);

        cache("TypeD", new TypeD(1, theTypeA));
        cache("TypeD", new TypeD(2, theTypeA));

        serializeAndDeserializeSnapshot();

        final List<TypeD> deserializedDs = getAll("TypeD");

        Assert.assertEquals(1, getAll("TypeA").size());
        Assert.assertEquals(2, deserializedDs.size());
        Assert.assertSame(deserializedDs.get(0).getTypeA(), deserializedDs.get(1).getTypeA());
    }

    @Test
    public void serializeNestedObjects() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        cache("TypeC", new TypeC(
                typeAMap(),
                Arrays.asList(
                        new TypeB(3, "four"),
                        new TypeB(5, "six")
                        )
                ));

        serializeAndDeserializeSnapshot();

        final TypeC deserializedC = (TypeC) getAll("TypeC").get(0);

        Assert.assertEquals(2, deserializedC.getTypeAMap().size());
        Assert.assertEquals(12, deserializedC.getTypeAMap().get("ED").getVal1());
        Assert.assertEquals(34, deserializedC.getTypeAMap().get("ED").getVal2());
        Assert.assertEquals(56, deserializedC.getTypeAMap().get("BR").getVal1());
        Assert.assertEquals(78, deserializedC.getTypeAMap().get("BR").getVal2());
        Assert.assertEquals(2, deserializedC.getTypeBs().size());
        Assert.assertEquals(3, deserializedC.getTypeBs().get(0).getVal1());
        Assert.assertEquals("four", deserializedC.getTypeBs().get(0).getVal2());
        Assert.assertEquals(5, deserializedC.getTypeBs().get(1).getVal1());
        Assert.assertEquals("six", deserializedC.getTypeBs().get(1).getVal2());

        Assert.assertSame(deserializedC.getTypeAMap(), getAll(TypeCSerializer.MAP_SERIALIZER.getName()).get(0));
        Assert.assertSame(deserializedC.getTypeBs(), getAll(TypeCSerializer.LIST_SERIALIZER.getName()).get(0));
        Assert.assertTrue(getAll("TypeB").contains(deserializedC.getTypeBs().get(0)));
        Assert.assertTrue(getAll("TypeB").contains(deserializedC.getTypeBs().get(1)));
    }

    private Map<String, TypeA> typeAMap() {
        final Map<String, TypeA> map = new HashMap<String, TypeA>();
        map.put("ED", new TypeA(12, 34));
        map.put("BR", new TypeA(56, 78));
        return map;
    }

}
