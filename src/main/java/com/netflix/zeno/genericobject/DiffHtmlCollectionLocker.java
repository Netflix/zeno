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
package com.netflix.zeno.genericobject;

import com.netflix.zeno.diff.DiffRecord;
import com.netflix.zeno.diff.DiffSerializationFramework;
import com.netflix.zeno.genericobject.GenericObject.Field;
import com.netflix.zeno.serializer.NFTypeSerializer;
import com.netflix.zeno.serializer.SerializerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This class will line up two collections so that the most similar items are paired together.<p/>
 *
 * This helps to eyeball the differences between two objects with collections in their hierarchies, without
 * requiring knowledge of the semantics of these objects.<p/>
 *
 * The similarity metric used here is the <a href="http://matpalm.com/resemblance/jaccard_coeff/">jaccard</a> distance.
 *
 * @author dkoszewnik
 *
 */
public class DiffHtmlCollectionLocker {

    private final DiffSerializationFramework diffFramework;

    public DiffHtmlCollectionLocker(SerializerFactory factory) {
        diffFramework = new DiffSerializationFramework(factory);
    }

    void lockCollectionFields(GenericObject from, GenericObject to) {
        List<Field> lockedFromFields = new ArrayList<Field>();
        List<Field> lockedToFields = new ArrayList<Field>();

        List<DiffRecord> fromDiffRecords = createDiffRecordList(from);
        List<DiffRecord> toDiffRecords = createDiffRecordList(to);

        JaccardMatrixPairwiseMatcher matcher = new JaccardMatrixPairwiseMatcher(from.getFields(), fromDiffRecords, to.getFields(), toDiffRecords);

        while (matcher.nextPair()) {
            lockedFromFields.add(matcher.getX());
            lockedToFields.add(matcher.getY());
        }

        from.setFields(lockedFromFields);
        to.setFields(lockedToFields);
    }

    private List<DiffRecord> createDiffRecordList(GenericObject from) {
        List<DiffRecord> diffRecords = new ArrayList<DiffRecord>();

        for (int i = 0; i < from.getFields().size(); i++) {
            Field field = from.getFields().get(i);
            if (field != null && field.getValue() != null) {
                DiffRecord rec = getDiffRecord(field);
                diffRecords.add(rec);
            } else {
                DiffRecord rec = new DiffRecord();
                rec.setSchema(from.getSchema());
                diffRecords.add(rec);
            }
        }

        return diffRecords;
    }

    private DiffRecord getDiffRecord(Field field) {
        GenericObject fieldValue = (GenericObject) field.getValue();
        DiffRecord rec = new DiffRecord();
        rec.setSchema(fieldValue.getSchema());
        if (field.getValue() instanceof GenericObject) {
            rec.setTopLevelSerializerName(fieldValue.getObjectType());
            ((NFTypeSerializer<Object>) diffFramework.getSerializer(fieldValue.getObjectType())).serialize(fieldValue.getActualObject(), rec);
        } else {
            rec.setTopLevelSerializerName("primitive");
            rec.serializePrimitive("value", field.getValue());
        }
        return rec;
    }

}
