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

import com.netflix.zeno.diff.DiffPropertyPath;
import com.netflix.zeno.diff.DiffRecord;
import com.netflix.zeno.diff.DiffRecordValueListMap;
import com.netflix.zeno.genericobject.GenericObject.Field;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * This class pulls out matches pairs of objects based on maximum jaccard similarity.<p/>
 *
 * For a good discussion of how jaccard similarity matrix works, see http://matpalm.com/resemblance/jaccard_distance/
 * 
 * @author dkoszewnik
 *
 */
public class JaccardMatrixPairwiseMatcher {

    private final List<Field> objects1;
    private final List<Field> objects2;

    private final float matrix[][];

    private int rowSize = 0;
    private int columnSize = 0;
    private final BitSet illegalColumns;
    private final BitSet illegalRows;

    private Field x;
    private Field y;


    public JaccardMatrixPairwiseMatcher(List<Field> objects1, List<DiffRecord> recs1, List<Field> objects2, List<DiffRecord> recs2) {
        this.objects1 = objects1;
        this.objects2 = objects2;

        columnSize = objects1.size();
        rowSize = objects2.size();

        matrix = new float[recs1.size()][recs2.size()];

        for(int i=0;i<columnSize;i++) {
            for(int j=0;j<rowSize;j++) {
                matrix[i][j] = calculateJaccardDistance(recs1.get(i), recs2.get(j));
            }
        }

        illegalColumns = new BitSet(columnSize);
        illegalRows = new BitSet();
    }

    private float calculateJaccardDistance(DiffRecord rec1, DiffRecord rec2) {
        DiffRecordValueListMap map1 = rec1.getValueListMap();
        DiffRecordValueListMap map2 = rec2.getValueListMap();

        int xorCardinality = 0;
        int unionCardinality = 0;


        for(DiffPropertyPath key : map1.keySet()) {
            List<Object> list1 = map1.getList(key);
            List<Object> list2 = map2.getList(key);

            if(list2 == null) {
                xorCardinality += list1.size();
            } else {
                list2 = new ArrayList<Object>(list2);
                for(Object o1 : list1) {
                    if(list2.contains(o1)) {
                        list2.remove(o1);
                        unionCardinality++;
                    } else {
                        xorCardinality++;
                    }
                }
            }
        }

        for(DiffPropertyPath key : map2.keySet()) {
            List<Object> list1 = map1.getList(key);

            if(list1 == null) {
                List<Object> list2 = map2.getList(key);
                xorCardinality += list2.size();
            }
        }

        if(xorCardinality == 0 && unionCardinality == 0) {
            return 0;
        }

        return ((float)xorCardinality) / ((float)(xorCardinality + unionCardinality));
    }

    public boolean nextPair() {
        int minColumn = -1;
        int minRow = -1;
        float minDistance = 1.0F;

        for(int i=0;i<columnSize;i++) {
            if(!illegalColumns.get(i)) {
                for(int j=0;j<rowSize;j++) {
                    if(!illegalRows.get(j)) {
                        if(matrix[i][j] < minDistance) {
                            minColumn = i;
                            minRow = j;
                            minDistance = matrix[i][j];
                        }
                    }
                }
            }
        }

        if(minDistance != 1.0F) {
            illegalColumns.set(minColumn);
            illegalRows.set(minRow);
            x = objects1.get(minColumn);
            y = objects2.get(minRow);
            return true;
        }

        for(int i=0;i<columnSize;i++) {
            if(!illegalColumns.get(i)) {
                illegalColumns.set(i);
                x = objects1.get(i);
                y = null;
                return true;
            }
        }


        for(int j=0;j<rowSize;j++) {
            if(!illegalRows.get(j)) {
                illegalRows.set(j);
                x = null;
                y = objects2.get(j);
                return true;
            }
        }

        x = null;
        y = null;
        return false;
    }

    public Field getX() {
        return x;
    }

    public Field getY() {
        return y;
    }

}
