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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.netflix.zeno.fastblob.record.FastBlobSchema;
import com.netflix.zeno.serializer.AbstractNFSerializationRecord;
import com.netflix.zeno.serializer.NFSerializationRecord;

import java.io.Writer;

/**
 *
 * @author tvaliulin
 *
 */
public class JsonWriteGenericRecord extends AbstractNFSerializationRecord {

    private static final JsonFactory s_jfactory = new JsonFactory();
    private static final DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();

    private JsonGenerator jGenerator;

    public JsonWriteGenericRecord(FastBlobSchema schema, Writer writer, boolean pretty) {
        this(schema, writer);
        if(pretty)
            jGenerator.setPrettyPrinter(prettyPrinter);
    }

    public JsonWriteGenericRecord(FastBlobSchema schema, Writer writer) {
        super(schema);
        try {
            jGenerator = s_jfactory.createGenerator(writer);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public JsonWriteGenericRecord(FastBlobSchema schema, JsonGenerator jGenerator) {
        super(schema);
        this.jGenerator = jGenerator;
    }

    public void open() {
        try {
            jGenerator.writeStartObject();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void close() {
        close(true);
    }

    public void close(boolean closeGenerator) {
        try {
            jGenerator.writeEndObject();
            if (closeGenerator) {
                jGenerator.close();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public JsonGenerator getGenerator() {
        return jGenerator;
    }
}
