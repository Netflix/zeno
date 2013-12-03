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

import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.testpojos.TypeA;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SerializerDeltaTest extends BlobSerializationGenericFrameworkAbstract {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void serializesAndDeserializesDeltas() throws Exception {
        serializationState = new FastBlobStateEngine(serializersFactory);

        cache("TypeA", new TypeA(1, 2));
        cache("TypeA", new TypeA(3, 4));

        serializeAndDeserializeSnapshot();

        cache("TypeA", new TypeA(1, 2));
        cache("TypeA", new TypeA(5, 6));

        serializeAndDeserializeDelta();

        final List<TypeA>allAs = getAll("TypeA");

        Assert.assertEquals(2, allAs.size());
        Assert.assertTrue(allAs.contains(new TypeA(1, 2)));
        Assert.assertTrue(allAs.contains(new TypeA(5, 6)));
        Assert.assertFalse(allAs.contains(new TypeA(3, 4)));
    }

}
