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

/**
 * Describes a historical transition an object went through when a data state was loaded.<p/>
 *
 * It's possible that no transition occurred for this object.
 *
 * @author dkoszewnik
 *
 * @param <T>
 */
public class DiffObjectHistoricalTransition<T> {

    private final String dataVersion;
    private final T before;
    private final T after;

    public DiffObjectHistoricalTransition(String version, T before, T after) {
        this.dataVersion = version;
        this.before = before;
        this.after = after;
    }

    public String getDataVersion() {
        return dataVersion;
    }

    public T getBefore() {
        return before;
    }

    public T getAfter() {
        return after;
    }

    public boolean itemChanged() {
        return before != after;
    }

}
