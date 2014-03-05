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

import com.netflix.zeno.serializer.NFSerializationRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * The GenericObject representation is used by the diff HTML generator.
 *
 * @author dkoszewnik
 *
 */
public class GenericObject extends NFSerializationRecord {

    private final String type;
    private final CollectionType collectionType;
    private final Object actualObject;
    private int collectionPosition;
    private List<Field> fields;

    public GenericObject(String objectType, Object actualObject) {
        this(objectType, CollectionType.NONE, actualObject);
    }

    public GenericObject(String objectType, CollectionType collectionType, Object actualObject) {
        this.type = objectType;
        this.collectionType = collectionType;
        this.actualObject = actualObject;
        this.fields = new ArrayList<Field>();
    }

    public void setCollectionPosition(int position) {
        this.collectionPosition = position;
    }

    public int getCollectionPosition() {
        return collectionPosition;
    }

    public void add(String fieldName, Object obj) {
        fields.add(new Field(fieldName, obj));
    }

    public void add(String fieldName, Object obj, int collectionPosition) {
        fields.add(new Field(fieldName, obj, collectionPosition));
    }

    public String getObjectType() {
        return type;
    }

    public CollectionType getCollectionType() {
        return collectionType;
    }

    public Object getActualObject() {
        return actualObject;
    }

    public List<Field> getFields() {
        return fields;
    }

    /// may be set after reordered by Jaccard Matrix pairwise matching for diff view
    void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public static class Field {
        private final String fieldName;
        private final Object value;
        private final int collectionPosition;

        public Field(String fieldName, Object value) {
            this(fieldName, value, 0);
        }

        public Field(String fieldName, Object value, int collectionPosition) {
            this.fieldName = fieldName;
            this.value = value;
            this.collectionPosition = collectionPosition;
        }

        public String getFieldName() {
            return fieldName;
        }

        public Object getValue() {
            return value;
        }

        public int getCollectionPosition() {
            return collectionPosition;
        }
    }

    public static enum CollectionType {
        NONE,
        MAP,
        COLLECTION
    }

}
