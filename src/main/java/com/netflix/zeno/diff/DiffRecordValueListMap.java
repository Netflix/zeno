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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A map of key/value pairs contained in a {@link DiffRecord}.<p/>
 * 
 * The DiffRecord flattens out a top-level object into key/value pairs.  This data structure
 * is used to hold the list of values corresponding to each "property path". <p/>
 * 
 * This data structure is intended to be reused after clear() is called with minimum object
 * creation overhead.
 *
 * @author dkoszewnik
 *
 */
public class DiffRecordValueListMap {

    private final Map<DiffPropertyPath, Integer> fieldValuesLists;
    private final List<List<Object>> valuesLists;
    private int nextValueIndex;

    public DiffRecordValueListMap() {
        this.fieldValuesLists = new HashMap<DiffPropertyPath, Integer>();
        this.valuesLists = new ArrayList<List<Object>>();
    }

    /**
     * Add a value to be associated with the supplied DiffPropertyPath
     */
    public void addValue(DiffPropertyPath path, Object obj) {
        getOrCreateList(path).add(obj);
    }

    /**
     * Get the list of values associated with the supplied DiffPropertyPath
     */
    public List<Object> getList(DiffPropertyPath path) {
        Integer listIndex = fieldValuesLists.get(path);
        if(listIndex == null)
            return null;
        return getList(listIndex.intValue());
    }

    private List<Object> getOrCreateList(DiffPropertyPath path) {
        Integer listIndex = fieldValuesLists.get(path);
        if(listIndex == null) {
            listIndex = Integer.valueOf(nextValueIndex++);
            /// create a copy of this DiffPropertyPath, as the propertyPath which is passed in
            /// is modified throughout the traversal.
            fieldValuesLists.put(path.copy(), listIndex);
        }
        return getList(listIndex.intValue());
    }

    private List<Object> getList(int listIndex) {
        while(valuesLists.size() <= listIndex) {
            valuesLists.add(new ArrayList<Object>());
        }

        return valuesLists.get(listIndex);
    }

    public void clear() {
        for(List<Object> list : valuesLists) {
            list.clear();
        }
        fieldValuesLists.clear();
        nextValueIndex = 0;
    }

    public Iterable<DiffPropertyPath> keySet() {
        return fieldValuesLists.keySet();
    }

}
