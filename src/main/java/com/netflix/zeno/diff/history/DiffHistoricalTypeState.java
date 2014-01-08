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
package com.netflix.zeno.diff.history;

import java.util.Map;
import java.util.Set;

/**
 * Contains the set of changes which occurred in a single type during a single data state update.  Changes
 * are broken down into:<p/>
 *
 * <ul>
 * <li>The set of newly created instances</li>
 * <li>The set of modified instances</li>
 * <li>The set of deleted instances</li>
 * </ul>
 *
 * @author dkoszewnik
 *
 */
public class DiffHistoricalTypeState<K, V> {

    private final Set<K> newObjects;
    private final Map<K, V> diffObjects;
    private final Map<K, V> deletedObjects;

    public DiffHistoricalTypeState(Set<K> newObjects, Map<K, V>diffObjects, Map<K, V> deletedObjects) {
        this.newObjects = newObjects;
        this.diffObjects = diffObjects;
        this.deletedObjects = deletedObjects;
    }

    public Set<K> getNewObjects() {
        return newObjects;
    }

    public Map<K, V> getDiffObjects() {
        return diffObjects;
    }

    public Map<K, V> getDeletedObjects() {
        return deletedObjects;
    }

    public int numChanges() {
        return newObjects.size() + diffObjects.size() + deletedObjects.size();
    }

}
