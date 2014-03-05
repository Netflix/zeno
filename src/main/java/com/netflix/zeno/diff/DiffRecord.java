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

import com.netflix.zeno.fastblob.record.schema.FastBlobSchema;
import com.netflix.zeno.serializer.NFSerializationRecord;
import com.netflix.zeno.serializer.NFTypeSerializer;

/**
 * A diff record represents the combined values for all fields at all levels in an {@link NFTypeSerializer} hierarchy.
 *
 * Conceptually, The diff of two Objects is calculated by the following process:
 * 1) reduce all properties in each Object to sets of key/value pairs.
 * 2) pull out matching pairs of key/value pairs from both Objects.
 * 3) When there are no more matches left, the diff score between the two Objects is sum of the remaining key/value pairs for both Objects.
 *
 * The DiffPropertyPath contained here is updated to reflect the current path during serialization.  This is an optimization which
 * allows us to not create new {@link DiffPropertyPath} objects at each step during serialization.  The {@link DiffRecordValueListMap} contains key/value
 * pairs.
 *
 * @author dkoszewnik
 *
 */
public class DiffRecord extends NFSerializationRecord {

    private final DiffPropertyPath propertyPath;
    private final DiffRecordValueListMap fieldValues;

    private FastBlobSchema schema;

    public DiffRecord() {
        this.propertyPath = new DiffPropertyPath();
        this.fieldValues = new DiffRecordValueListMap();
    }

    public void setSchema(FastBlobSchema schema) {
        this.schema = schema;
    }

    public FastBlobSchema getSchema() {
        return schema;
    }

    public void setTopLevelSerializerName(String topNodeSerializer) {
        propertyPath.setTopNodeSerializer(topNodeSerializer);
    }

    public void serializeObject(String fieldName) {
        propertyPath.addBreadcrumb(fieldName);
    }

    public void finishedObject() {
        propertyPath.removeBreadcrumb();
    }

    public void serializePrimitive(String fieldName, Object value) {
        propertyPath.addBreadcrumb(fieldName);
        fieldValues.addValue(propertyPath, value);
        propertyPath.removeBreadcrumb();
    }

    public void clear() {
        propertyPath.reset();
        fieldValues.clear();
    }

    public DiffRecordValueListMap getValueListMap() {
        return fieldValues;
    }

    DiffRecordValueListMap getFieldValues() {
        return fieldValues;
    }


}
