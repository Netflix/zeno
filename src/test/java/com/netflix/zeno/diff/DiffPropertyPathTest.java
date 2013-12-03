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

import org.junit.Assert;
import org.junit.Test;

public class DiffPropertyPathTest {

    @Test
    public void hashCodeIsConsistent() {
        DiffPropertyPath prop = new DiffPropertyPath();

        prop.setTopNodeSerializer("topNodeSerializer");

        prop.addBreadcrumb("test");
        prop.addBreadcrumb("hash");

        int firstHashCode = prop.hashCode();

        prop.addBreadcrumb("code");

        int secondHashCode = prop.hashCode();

        prop.addBreadcrumb("is");
        prop.addBreadcrumb("consistent");

        int thirdHashCode = prop.hashCode();

        Assert.assertEquals(thirdHashCode, prop.hashCode());

        prop.removeBreadcrumb();
        prop.removeBreadcrumb();

        Assert.assertEquals(secondHashCode, prop.hashCode());

        prop.removeBreadcrumb();

        Assert.assertEquals(firstHashCode, prop.hashCode());

        prop.addBreadcrumb("code");

        Assert.assertEquals(secondHashCode, prop.hashCode());

        prop.addBreadcrumb("is");

        Assert.assertFalse(thirdHashCode == prop.hashCode());

        prop.addBreadcrumb("consistent");

        Assert.assertEquals(thirdHashCode, prop.hashCode());
    }

}
