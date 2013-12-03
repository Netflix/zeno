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

import com.netflix.zeno.diff.TypeDiff.ObjectDiffScore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * A diff report is the result of a diff operation.
 *
 * This data structure allows for investigation of the differences found between two deserialized blobs.
 *
 * The diff is organized into "extra" objects, which are objects which had no match, and "different" object pairs,
 * which are pairs of objects which were matched, but had differences.
 *
 * The DiffReport contains the sum of all diffs, the sum of all "extra" objects, and a series of TypeDiff objects, one per
 * {@link TypeDiffInstruction} supplied to the {@link DiffInstruction} which generated this report.
 * 
 * See the class DiffExample under source folder src/examples/java for an example of how to use this.
 *
 * @author dkoszewnik
 *
 */
public class DiffReport {

    private final DiffHeader header;
    private final List<TypeDiff<?>> typeDiffs;
    private int totalDiffs;
    private int totalExtra;

    public DiffReport(final DiffHeader header, final List<TypeDiff<?>> typeDiffs) {
        this.header = header;
        this.typeDiffs = typeDiffs;

        for(final TypeDiff<?> td : typeDiffs) {
            for(final ObjectDiffScore<?> objectDiffScore : td.getDiffObjects()) {
                totalDiffs += objectDiffScore.getScore();
            }
            totalExtra += td.getExtraInFrom().size() + td.getExtraInTo().size();
        }
    }

    @SuppressWarnings("unchecked")
    public <T> TypeDiff<T> getTypeDiff(final String topLevelSerializer) {
        for(final TypeDiff<?> typeDiff : typeDiffs) {
            if(typeDiff.getTopNodeSerializer().equals(topLevelSerializer)) {
                return (TypeDiff<T>)typeDiff;
            }
        }
        return null;
    }

    public List<TypeDiff<?>> getTypeDiffsSortedByDiffScore() {
        final List<TypeDiff<?>> typeDiffs = new ArrayList<TypeDiff<?>>(this.typeDiffs.size());
        typeDiffs.addAll(this.typeDiffs);
        Collections.sort(typeDiffs, new Comparator<TypeDiff<?>>() {
            @Override
            public int compare(final TypeDiff<?> o1, final TypeDiff<?> o2) {
                return o2.getTotalDiffs() - o1.getTotalDiffs();
            }
        });
        return typeDiffs;
    }

    public List<TypeDiff<?>> getTypeDiffsSortedByExtraObjects() {
        final List<TypeDiff<?>> typeDiffs = new ArrayList<TypeDiff<?>>(this.typeDiffs.size());
        typeDiffs.addAll(this.typeDiffs);
        Collections.sort(typeDiffs, new Comparator<TypeDiff<?>>() {
            @Override
            public int compare(final TypeDiff<?> o1, final TypeDiff<?> o2) {
                final int extra2 = o2.getExtraInFrom().size() + o2.getExtraInTo().size();
                final int extra1 = o1.getExtraInFrom().size() + o1.getExtraInTo().size();
                return extra2 - extra1;
            }
        });
        return typeDiffs;

    }

    public List<TypeDiff<?>> getTypeDiffsSortedByMissingFromObjects() {
        final List<TypeDiff<?>> typeDiffs = new ArrayList<TypeDiff<?>>(this.typeDiffs.size());
        typeDiffs.addAll(this.typeDiffs);
        Collections.sort(typeDiffs, new Comparator<TypeDiff<?>>() {
            @Override
            public int compare(final TypeDiff<?> o1, final TypeDiff<?> o2) {
                final int extra2 = o2.getExtraInFrom().size();
                final int extra1 = o1.getExtraInFrom().size();
                return extra2 - extra1;
            }
        });
        return typeDiffs;
    }

    public List<TypeDiff<?>> getTypeDiffsSortedByMissingToObjects() {
        final List<TypeDiff<?>> typeDiffs = new ArrayList<TypeDiff<?>>(this.typeDiffs.size());
        typeDiffs.addAll(this.typeDiffs);
        Collections.sort(typeDiffs, new Comparator<TypeDiff<?>>() {
            @Override
            public int compare(final TypeDiff<?> o1, final TypeDiff<?> o2) {
                final int extra2 = o2.getExtraInTo().size();
                final int extra1 = o1.getExtraInTo().size();
                return extra2 - extra1;
            }
        });
        return typeDiffs;
    }

    public int getTotalDiffs() {
        return totalDiffs;
    }

    public int getTotalExtra() {
        return totalExtra;
    }

    public DiffHeader getHeader() {
        return header;
    }

}
