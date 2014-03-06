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
package com.netflix.zeno.serializer;

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;

/**
 * An NFSerializationRecord is responsible for tracking state during traversal via a FrameworkSerializer.
 *
 * The minimum state to track is a FastBlobSchema for each level in the hierarchy, so that sub-object type names may be interrogated for each field.
 */
public abstract class NFSerializationRecord {

   private FastBlobSchema schema;

   public void setSchema(FastBlobSchema schema) {
       this.schema = schema;
   }

   public FastBlobSchema getSchema() {
       return schema;
   }

   public String getObjectType(String schemaField) {
       return schema.getObjectType(schemaField);
   }
}
