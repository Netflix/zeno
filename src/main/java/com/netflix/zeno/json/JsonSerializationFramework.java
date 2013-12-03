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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializationFramework;
import com.netflix.zeno.serializer.SerializerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 *
 * Serializes and deserializes JSON based on object instance contents.<p/>
 *
 * Usage is detailed in the <a href="https://github.com/Netflix/zeno/wiki">documentation</a> 
 * on the page <a href="https://github.com/Netflix/zeno/wiki/Creating-json-data">creating json data</a>.<p/>
 * 
 * Please see JSONSerializationExample in the source folder src/examples/java for example usage.
 *
 * @author tvaliulin
 *
 */
public class JsonSerializationFramework extends SerializationFramework {

    public JsonSerializationFramework(SerializerFactory factory) {
        super(factory);
        this.frameworkSerializer = new JsonFrameworkSerializer(this);
        this.frameworkDeserializer = new JsonFrameworkDeserializer(this);
    }

    public <T> String serializeAsJson(String type, T object) {
        return serializeAsJson(type, object, true);
    }

    public <T> String serializeAsJson(String type, T object, boolean pretty) {
        StringWriter writer = new StringWriter();

        JsonWriteGenericRecord record = new JsonWriteGenericRecord(writer, pretty);

        record.open();
        getSerializer(type).serialize(object, record);
        record.close();
        writer.flush();

        return writer.toString();
    }




    public <T> T deserializeJson(String type, String json) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(json);
        JsonReadGenericRecord readRecord = new JsonReadGenericRecord(node);
        NFTypeSerializer<T> serializer = getSerializer(type);
        T object = serializer.deserialize(readRecord);

        return object;
    }

}
