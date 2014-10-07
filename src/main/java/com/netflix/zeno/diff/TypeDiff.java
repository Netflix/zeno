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

import com.netflix.zeno.serializer.NFTypeSerializer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This Object represents the result of a diff for a specific type in the {@link NFTypeSerializer} object hierarchy.
 *
 * It is organized into the following:
 * - The list of Objects which were extra in the "from" FastBlobStateEngine (had no corresponding Object in the "to" engine)
 * - The list of Objects which were extra in the "to" FastBlobStateEngine (had no corresponding Object in the "from" engine)
 * - a list of {@link ObjectDiffScore}s, each of which contains a matching pair of Objects and the diff score between them.
 *
 * (from {@link DiffRecord})
 * Conceptually, The diff of two Objects is calculated by the following process:
 * 1) reduce all properties in each Object to sets of key/value pairs.
 * 2) pull out matching pairs of key/value pairs from both Objects.
 * 3) When there are no more matches left, the diff score between the two Objects is sum of the remaining key/value pairs for both Objects.
 *
 * @author dkoszewnik
 *
 */
public class TypeDiff<T> {

    private final String topNodeSerializer;

    private final Map<DiffPropertyPath, FieldDiffScore<T>> fieldDifferences;

    private final List<T> extraInFrom;
    private final List<T> extraInTo;

    private final List<ObjectDiffScore<T>> objectDiffs;

    private int itemCountFrom;
    private int itemCountTo;

    public TypeDiff(String topNodeSerializer) {
        fieldDifferences = new HashMap<DiffPropertyPath, FieldDiffScore<T>>();
        extraInFrom = new ArrayList<T>();
        extraInTo = new ArrayList<T>();
        objectDiffs = new ArrayList<ObjectDiffScore<T>>();
        this.topNodeSerializer = topNodeSerializer;
    }

    TypeDiff(String topNodeSerializer, List<T> missingFrom, List<T> missingTo, List<ObjectDiffScore<T>> objectDiffs, Map<DiffPropertyPath, FieldDiffScore<T>> fieldDifferences, int itemCountFrom, int itemCountTo) {
        this.topNodeSerializer = topNodeSerializer;
        this.extraInFrom = missingFrom;
        this.extraInTo = missingTo;
        this.objectDiffs = objectDiffs;
        this.fieldDifferences = fieldDifferences;
        this.itemCountFrom = itemCountFrom;
        this.itemCountTo = itemCountTo;
    }

    public void addFieldObjectDiffScore(DiffPropertyPath fieldBreadcrumbs, T to, T from, int diffScore) {
        if(diffScore > 0) {
            ObjectDiffScore<T> fieldDiffScore = new ObjectDiffScore<T>(from, to, diffScore);
            FieldDiffScore<T> fieldDiff = getFieldDiffScore(fieldBreadcrumbs);
            fieldDiff.addObjectDiffScore(fieldDiffScore);
        }
    }

    public void incrementFieldScores(DiffPropertyPath fieldBreadcrumbs, int diffIncrement, int totalIncrement) {
        if(diffIncrement != 0 || totalIncrement != 0) {
            FieldDiffScore<T> fieldDiff = getFieldDiffScore(fieldBreadcrumbs);
            fieldDiff.incrementDiffCountBy(diffIncrement);
            fieldDiff.incrementTotalCountBy(totalIncrement);
        }
    }

    public void incrementFieldDiff(DiffPropertyPath fieldBreadcrumbs, int increment) {
        if(increment != 0) {
            FieldDiffScore<T> fieldDiff = getFieldDiffScore(fieldBreadcrumbs);
            fieldDiff.incrementDiffCountBy(increment);
        }
    }


    public void incrementFieldTotal(DiffPropertyPath fieldBreadcrumbs, int increment) {
        if(increment != 0) {
            FieldDiffScore<T> fieldDiff = getFieldDiffScore(fieldBreadcrumbs);
            fieldDiff.incrementTotalCountBy(increment);
        }
    }

    private FieldDiffScore<T> getFieldDiffScore(DiffPropertyPath fieldBreadcrumbs) {
        FieldDiffScore<T> counter = fieldDifferences.get(fieldBreadcrumbs);
        if(counter == null) {
            counter = new FieldDiffScore<T>();
            fieldDifferences.put(fieldBreadcrumbs, counter);
        }
        return counter;
    }

    public void addExtraInFrom(T missing) {
        extraInFrom.add(missing);
    }

    public void addExtraInTo(T missing) {
        extraInTo.add(missing);
    }

    public void addDiffObject(T from, T to, int score) {
        objectDiffs.add(new ObjectDiffScore<T>(from, to, score));
    }

    public void incrementFrom() {
        itemCountFrom++;
    }

    public void incrementFrom(int byCount) {
        itemCountFrom += byCount;
    }

    public void incrementTo() {
        itemCountTo++;
    }

    public void incrementTo(int byCount) {
        itemCountTo += byCount;
    }

    public String getTopNodeSerializer() {
        return topNodeSerializer;
    }

    public List<T> getExtraInFrom() {
        return extraInFrom;
    }

    public List<T> getExtraInTo() {
        return extraInTo;
    }

    public int getItemCountFrom() {
        return itemCountFrom;
    }

    public int getItemCountTo() {
        return itemCountTo;
    }

    public int getTotalDiffs() {
        int totalDiffs = 0;
        for(ObjectDiffScore<?> objectDiffScore : objectDiffs) {
            totalDiffs += objectDiffScore.getScore();
        }
        return totalDiffs;
    }

    public List<FieldDiff<T>> getSortedFieldDifferencesDescending() {
        List<FieldDiff<T>> fieldDiffs = new ArrayList<FieldDiff<T>>(fieldDifferences.size());

        for(DiffPropertyPath key : fieldDifferences.keySet()) {
            fieldDiffs.add(new FieldDiff<T>(key, fieldDifferences.get(key)));
        }

        Collections.sort(fieldDiffs);

        return fieldDiffs;
    }

    public Map<DiffPropertyPath, FieldDiffScore<T>> getFieldDifferences() {
        return fieldDifferences;
    }

    public List<ObjectDiffScore<T>> getDiffObjects() {
        return objectDiffs;
    }

    public List<ObjectDiffScore<T>> getSortedDiffObjects() {
        List<ObjectDiffScore<T>> sortedList = new ArrayList<ObjectDiffScore<T>>(objectDiffs.size());
        sortedList.addAll(objectDiffs);
        Collections.sort(sortedList);
        return sortedList;
    }

    public List<ObjectDiffScore<T>> getSortedDiffObjectsByFields(List<DiffPropertyPath> includeFields) {
        Map<ObjectDiffScore<T>, ObjectDiffScore<T>> aggregatedScores = new HashMap<ObjectDiffScore<T>, ObjectDiffScore<T>>();

        for(DiffPropertyPath field : includeFields) {
            FieldDiffScore<T> fieldDiffScore = fieldDifferences.get(field);
            if(fieldDiffScore != null) {
                for(ObjectDiffScore<T> fieldObjectDiff : fieldDiffScore.getDiffScores()) {
                    ObjectDiffScore<T> objectDiffCopy = aggregatedScores.get(fieldObjectDiff);
                    if(objectDiffCopy == null) {
                        objectDiffCopy = new ObjectDiffScore<T>(fieldObjectDiff.getFrom(), fieldObjectDiff.getTo(), 0);
                        aggregatedScores.put(objectDiffCopy, objectDiffCopy);
                    }
                    objectDiffCopy.incrementScoreBy(fieldObjectDiff.getScore());
                }
            }
        }

        List<ObjectDiffScore<T>> scores = new ArrayList<ObjectDiffScore<T>>(aggregatedScores.keySet());
        Collections.sort(scores);
        return scores;
    }


    public static class FieldDiff<T> implements Comparable<FieldDiff<T>> {
        private final DiffPropertyPath propertyPath;
        private final FieldDiffScore<T> diffScore;

        public FieldDiff(DiffPropertyPath propertyPath, FieldDiffScore<T> diffScore) {
            this.propertyPath = propertyPath;
            this.diffScore = diffScore;
        }

        public DiffPropertyPath getPropertyPath() {
            return propertyPath;
        }

        /**
         * @deprecated use getPropertyPath() instead
         */
        @Deprecated
        public DiffPropertyPath getBreadcrumbs() {
            return propertyPath;
        }

        public FieldDiffScore<T> getDiffScore() {
            return diffScore;
        }

        @Override
        public int compareTo(FieldDiff<T> o) {
            return diffScore.compareTo(o.diffScore);
        }

        @Override
        public String toString() {
            return propertyPath.toString() + ": " + diffScore.toString();
        }
    }

    public static class FieldDiffScore<T> implements Comparable<FieldDiffScore<T>> {
        private int diffCount;
        private int totalCount;
        private final List<ObjectDiffScore<T>> objectScores;

        public FieldDiffScore() {
            this.objectScores = new ArrayList<ObjectDiffScore<T>>();
        }

        public void incrementDiffCountBy(int count) {
            diffCount += count;
        }

        public void incrementTotalCountBy(int count) {
            totalCount += count;
        }

        public int getDiffCount() {
            return diffCount;
        }

        public int getTotalCount() {
            return totalCount;
        }

        public List<ObjectDiffScore<T>> getDiffScores() {
            return objectScores;
        }

        public double getDiffPercent() {
            return (double)diffCount / (double)totalCount;
        }

        public void addObjectDiffScore(ObjectDiffScore<T> score) {
            objectScores.add(score);
        }

        @Override
        public int compareTo(FieldDiffScore<T> o) {
            double thisDiffPercent = getDiffPercent();
            double otherDiffPercent = o.getDiffPercent();

            if(thisDiffPercent == otherDiffPercent)
                return 0;

            return thisDiffPercent > otherDiffPercent ? -1 : 1;
        }


        @Override
        public String toString() {
            NumberFormat nf = NumberFormat.getInstance();
            nf.setMaximumFractionDigits(3);


            return nf.format(getDiffPercent() * 100) + "% (" + diffCount + "/" + totalCount + ")";
        }

    }

    public static class ObjectDiffScore<T> implements Comparable<ObjectDiffScore<T>>{
        private final T fromObject;
        private final T toObject;
        private int score;

        public ObjectDiffScore(T fromObject, T toObject, int score) {
            this.fromObject = fromObject;
            this.toObject = toObject;
            this.score = score;
        }

        public T getFrom() {
            return fromObject;
        }

        public T getTo() {
            return toObject;
        }

        public int getScore() {
            return score;
        }

        private void incrementScoreBy(int increment) {
            score += increment;
        }

        @Override
        public int compareTo(ObjectDiffScore<T> o) {
            return o.score - score;
        }

        @Override
        public int hashCode() {
            return fromObject.hashCode() + (31 * toObject.hashCode());
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean equals(Object other) {
            if(other instanceof ObjectDiffScore) {
                ObjectDiffScore<T> otherScore = (ObjectDiffScore<T>) other;
                return otherScore.getFrom().equals(fromObject) && otherScore.getTo().equals(toObject);
            }
            return false;
        }
    }

}
