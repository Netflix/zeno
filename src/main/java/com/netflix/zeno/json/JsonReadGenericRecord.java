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
package com.netflix.zeno.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFDeserializationRecord;

/**
 * @author tvaliulin
 *
 */
public class JsonReadGenericRecord extends NFDeserializationRecord {

    private final JsonNode node;

    public JsonReadGenericRecord(FastBlobSchema schema, JsonNode node) {
        super(schema);
        this.node = node;
    }

    JsonNode getNode() {
        return node;
    }
}
