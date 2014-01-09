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

import com.netflix.zeno.genericobject.GenericObject.CollectionType;
import com.netflix.zeno.genericobject.GenericObject.Field;
import com.netflix.zeno.serializer.SerializerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This class generates HTML describing the differences between two GenericObjects.
 *
 * @author dkoszewnik
 *
 */
/*
 * This is extremely messy code and is ripe for replacement.
 *
 * It is the result of an afternoon spike several months ago and has not yet needed any updates.
 */
public class DiffHtmlGenerator {

    private final DiffHtmlCollectionLocker locker;
    private final GenericObjectSerializationFramework genericObjectFramework;

    private final boolean moreAtFromLevels[] = new boolean[1024];
    private final boolean moreAtToLevels[] = new boolean[1024];

    private int hierarchyLevel = 1;

    /**
     * Instantiate with the SerializerFactory describing an object model.
     */
    public DiffHtmlGenerator(SerializerFactory factory) {
        locker = new DiffHtmlCollectionLocker(factory);
        genericObjectFramework = new GenericObjectSerializationFramework(factory);
    }

    /**
     * Generate the HTML difference between two objects.
     *
     * @param objectType - The NFTypeSerializer name of the objects
     * @param from - The first object to diff
     * @param to - The second object to diff
     * @return
     */
    public String generateDiff(String objectType, Object from, Object to) {
        GenericObject fromGenericObject = from == null ? null : genericObjectFramework.serialize(from, objectType);
        GenericObject toGenericObject = to == null ? null : genericObjectFramework.serialize(to, objectType);

        return generateDiff(fromGenericObject, toGenericObject);
    }

    /**
     * Generate the HTML difference between two GenericObjects.
     *
     * @return
     */
    public String generateDiff(GenericObject from, GenericObject to) {
        StringBuilder builder = new StringBuilder();

        builder.append("<table class=\"nomargin diff\">");
        builder.append("<thead>");
        builder.append("<tr>");
        builder.append("<th/>");
        builder.append("<th class=\"texttitle\">From</th>");
        builder.append("<th/>");
        builder.append("<th class=\"texttitle\">To</th>");
        builder.append("</tr>");
        builder.append("</thead>");

        builder.append("<tbody>");

        writeDiff(builder, from, to);

        builder.append("</tbody>");
        builder.append("</table>");

        return builder.toString();
    }


    private void writeDiff(StringBuilder builder, GenericObject from, GenericObject to) {
        if(from != null && to != null) {
            writeDiffBothObjectsPresent(builder, from, to);
        } else if(from != null || to != null) {
            writeDiffOneObjectNull(builder, from, to);
        }
    }

    private void writeDiffBothObjectsPresent(StringBuilder builder, GenericObject from, GenericObject to) {
        if(from.getCollectionType() == CollectionType.COLLECTION) {
            writeCollectionDiff(builder, from, to);
        } else if(from.getCollectionType() == CollectionType.MAP) {
            writeMapDiff(builder, from, to);
        } else {
            writeObjectDiff(builder, from, to);
        }
    }

    private void writeCollectionDiff(StringBuilder builder, GenericObject from, GenericObject to) {
        locker.lockCollectionFields(from, to);

        /// both objects' fields length should be the same after the lockCollectionFields operation
        for(int i=0;i<from.getFields().size();i++) {
            Field fromField = from.getFields().get(i);
            Field toField = to.getFields().get(i);
            boolean moreFromFields = moreCollectionFields(from.getFields(), i);
            boolean moreToFields = moreCollectionFields(to.getFields(), i);

            appendField(builder, fromField, toField, moreFromFields, moreToFields);
        }
    }

    private void writeMapDiff(StringBuilder builder, GenericObject from, GenericObject to) {
        sortMapFieldsByKey(from);
        sortMapFieldsByKey(to);

        int fromCounter = 0;
        int toCounter = 0;

        while(fromCounter < from.getFields().size() || toCounter < to.getFields().size()) {
            Field fromField = fromCounter < from.getFields().size() ? from.getFields().get(fromCounter) : null;
            Field toField = toCounter < to.getFields().size() ? to.getFields().get(toCounter) : null;

            int comparison = mapFieldComparator.compare(fromField, toField);

            boolean moreFromFields = moreCollectionFields(from.getFields(), fromCounter);
            boolean moreToFields = moreCollectionFields(to.getFields(), toCounter);

            if(comparison == 0) {
                appendField(builder, fromField, toField, moreFromFields, moreToFields);
                fromCounter++;
                toCounter++;
            } else if(comparison < 0) {
                appendField(builder, fromField, null, moreFromFields, toField != null);
                fromCounter++;
            } else {
                appendField(builder, null, toField, fromField != null, moreToFields);
                toCounter++;
            }
        }
    }

    private boolean moreCollectionFields(List<Field> fieldList, int position) {
        if(position >= fieldList.size())
            return false;

        for(int i=position+1;i<fieldList.size();i++) {
            if(fieldList.get(i) != null)
                return true;
        }

        return false;
    }

    private void writeObjectDiff(StringBuilder builder, GenericObject from, GenericObject to) {
        /// objects of the same type should always have the same number of fields.
        for(int i=0;i<from.getFields().size();i++) {
            Field fromField = from.getFields().get(i);
            Field toField = to.getFields().get(i);
            boolean moreFields = i != (from.getFields().size() - 1);

            appendField(builder, fromField, toField, moreFields, moreFields);
        }
    }


    private void sortMapFieldsByKey(GenericObject map) {
        Collections.sort(map.getFields(), mapFieldComparator);
    }



    private void writeDiffOneObjectNull(StringBuilder builder, GenericObject from, GenericObject to) {
        if(from != null) {
            for(int i=0;i<from.getFields().size();i++) {
                boolean moreFields = i != (from.getFields().size() - 1);
                appendField(builder, from.getFields().get(i), null, moreFields, false);
            }
        } else {
            for(int i=0;i<to.getFields().size();i++) {
                boolean moreFields = i != (to.getFields().size() - 1);
                appendField(builder, null, to.getFields().get(i), false, moreFields);
            }
        }
    }

    private void appendField(StringBuilder builder, Field fromField, Field toField, boolean moreAtFromLevel, boolean moreAtToLevel) {
        moreAtFromLevels[hierarchyLevel] = moreAtFromLevel;
        moreAtToLevels[hierarchyLevel] = moreAtToLevel;

        Object nonNullValue = getNonNullValue(fromField, toField);

        builder.append("<tr>");
        if(fromField != null && fromField.getCollectionPosition() != 0)
            builder.append("<th>").append(fromField.getCollectionPosition()).append("</th>");
        else
            builder.append("<th/>");
        builder.append("<td class=\"").append(cssClass(fromField, toField, "delete")).append("\">");
        if(fromField != null) {
            if(nonNullValue instanceof GenericObject) {
                openNewObject(builder, fromField.getFieldName(), ((GenericObject) nonNullValue).getObjectType(), moreAtFromLevel, true);
            } else {
                appendFieldValue(builder, fromField.getFieldName(), fromField.getValue(), moreAtFromLevel, true);
            }
        } else {
            appendEmptyHierarchyLevel(builder, moreAtFromLevels);
        }
        builder.append("</td>");
        if(toField != null && toField.getCollectionPosition() != 0)
            builder.append("<th>").append(toField.getCollectionPosition()).append("</th>");
        else
            builder.append("<th/>");
        builder.append("<td class=\"").append(cssClass(toField, fromField, "insert")).append("\">");
        if(toField != null) {
            if(nonNullValue instanceof GenericObject) {
                openNewObject(builder, toField.getFieldName(), ((GenericObject) nonNullValue).getObjectType(), moreAtToLevel, false);
            } else {
                appendFieldValue(builder, toField.getFieldName(), toField.getValue(), moreAtToLevel, false);
            }
        } else {
            appendEmptyHierarchyLevel(builder, moreAtToLevels);
        }
        builder.append("</td>");
        builder.append("</tr>");

        if(nonNullValue instanceof GenericObject) {
            hierarchyLevel++;
            writeDiff(builder, (GenericObject)fieldValue(fromField), (GenericObject)fieldValue(toField));
            hierarchyLevel--;
        }

        moreAtFromLevels[hierarchyLevel] = false;
        moreAtToLevels[hierarchyLevel] = false;
    }

    private Object fieldValue(Field field) {
        if(field == null)
            return null;
        return field.getValue();
    }

    private Object getNonNullValue(Field fromField, Field toField) {
        if(fromField != null && fromField.getValue() != null)
            return fromField.getValue();
        if(toField != null)
            return toField.getValue();
        return null;
    }

    private void openNewObject(StringBuilder builder, String fieldName, String typeName, boolean lastFieldAtLevel, boolean from) {
        appendHierarchyLevel(builder, true, hierarchyLevel, lastFieldAtLevel, from);

        builder.append(fieldName).append(": ").append("(").append(typeName).append(")\n");
    }


    private String cssClass(Field field1, Field field2, String missingCssClass) {
        if(field1 == null)
            return "empty";
        if(field2 == null)
            return missingCssClass;
        if(field1.getValue() == null && field2.getValue() != null)
            return "replace";
        if(field1.getValue() == null && field2.getValue() == null)
            return "equal";
        if(field1.getValue() != null && field2.getValue() == null)
            return "replace";
        if(!(field1.getValue() instanceof GenericObject)) {
            if(!field1.getValue().equals(field2.getValue()))
                return "replace";
        }
        return "equal";
    }

    private void appendFieldValue(StringBuilder builder, String fieldName, Object value, boolean moreFieldsAtLevel, boolean from) {
        appendHierarchyLevel(builder, false, hierarchyLevel, moreFieldsAtLevel, from);
        builder.append(fieldName).append(": ").append(value).append("\n");
    }


    private void appendHierarchyLevel(StringBuilder builder, boolean objectField, int hierarchyLevel, boolean moreFieldsAtLevel, boolean from) {
        boolean levelGuide[] = from ? moreAtFromLevels : moreAtToLevels;
        for(int i=1;i<hierarchyLevel;i++) {
            if(levelGuide[i]) {
                builder.append(".&#x2502;");
            } else {
                builder.append("..");
            }
        }
        if(objectField) {
            if(moreFieldsAtLevel)
                builder.append(".&#x251D;&#x2501;&#x252F;&#x2501;>");
            else
                builder.append(".&#x2515;&#x2501;&#x252F;&#x2501;>");
        } else {
            if(moreFieldsAtLevel)
                builder.append(".&#x251C;&#x2500;&#x2500;&#x2500;>");
            else
                builder.append(".&#x2514;&#x2500;&#x2500;&#x2500;>");
        }
    }

    private void appendEmptyHierarchyLevel(StringBuilder builder, boolean[] levelGuide) {
        for(int i=1;i<=hierarchyLevel;i++) {
            if(levelGuide[i]) {
                builder.append(" &#x2502;");
            } else {
                for(int j=i;j<=hierarchyLevel;j++) {
                    if(levelGuide[j]) {
                        builder.append("  ");
                        break;
                    }
                }
            }
        }
    }

    private static final Comparator<Field> mapFieldComparator = new Comparator<Field>() {
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public int compare(Field o1, Field o2) {
            if(o1 == null && o2 == null)
                return 0;
            if(o1 == null)
                return 1;
            if(o2 == null)
                return -1;

            Object key1 = getKey(o1);
            Object key2 = getKey(o2);

            if(key1 instanceof Comparable) {
                return ((Comparable) key1).compareTo(key2);
            }

            return key1.hashCode() - key2.hashCode();
        }
    };

    private static Object getKey(Field entryField) {
        GenericObject entryObject = (GenericObject) entryField.getValue();
        Field keyField = entryObject.getFields().get(0);
        GenericObject keyObject = (GenericObject)keyField.getValue();
        return keyObject.getActualObject();
    }


}
