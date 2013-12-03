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

import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

/**
 * This class is used in the context of the Zeno diff operation.  It's unlikely that users will
 * want to use this directly.  Instead, TypeDiffOperation contains the main interface for performing a diff
 * between two arbitrary data states.<p/>
 * 
 * See the class DiffExample under source folder src/examples/java for an example of how to perform a diff on two data sets.
 *
 * @author dkoszewnik
 *
 */
public class DiffSerializationFramework extends SerializationFramework {

    public DiffSerializationFramework(SerializerFactory serializerFactory) {
        super(serializerFactory);
        this.frameworkSerializer = new DiffFrameworkSerializer(this);
    }

}
