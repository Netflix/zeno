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

import com.netflix.zeno.diff.DiffInstruction;
import com.netflix.zeno.diff.TypeDiffInstruction;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.util.SimultaneousExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A data structure to track the history of changes in a single FastBlobStateEngine.<p/>
 *
 * Each time the state engine consumes a blob file, call "addState()".  Then, at any time, pass in a key for an object identified in the
 * TypeDiffInstruction to retrieve a historical record of that data.<p/>
 *
 * This data structure retains a history of all changes in a data set across some specified number of rolling updates.  Because this retains a
 * large amount of data, it can consume a significant memory footprint, and resource availability should be planned accordingly.<p/>
 *
 * This class takes advantage of the guarantee that two identical objects across adjacent data states will be the same instance.  Comparisons
 * can therefore be done with ==, rather than checking for identical serialized representations.
 *
 * @author dkoszewnik
 *
 */
public class DiffHistoryTracker {

    private final int historySizeToKeep;
    private final FastBlobStateEngine stateEngine;
    private final LinkedList<DiffHistoricalState> historicalStates;
    private final Map<String, Map<String, String>> historicalStateHeaderTags;
    private final TypeDiffInstruction<?> typeDiffInstructions[];
    private DiffHistoryDataState currentDataState;

    /**
     *
     * @param numStatesToKeep - The number of historical states to keep
     * @param stateEngine - The state engine to track the history of
     * @param diffInstruction - The set of key extractions for types in the object model.
     */
    public DiffHistoryTracker(int numStatesToKeep, FastBlobStateEngine stateEngine, DiffInstruction diffInstruction) {
        this.historySizeToKeep = numStatesToKeep;
        this.stateEngine = stateEngine;
        this.historicalStates = new LinkedList<DiffHistoricalState>();
        this.historicalStateHeaderTags = new ConcurrentHashMap<String, Map<String,String>>();
        this.typeDiffInstructions = diffInstruction.getTypeInstructions();
    }

    /**
     * Call this method after new data has been loaded by the FastBlobStateEngine.  This will add a historical record
     * of the differences between the previous state and this new state.
     */
    public void addState() {
        DiffHistoryDataState nextState = new DiffHistoryDataState(stateEngine, typeDiffInstructions);

        if(currentDataState != null)
            newHistoricalState(currentDataState, nextState);

        currentDataState = nextState;
    }

    private void newHistoricalState(final DiffHistoryDataState from, final DiffHistoryDataState to) {
        final DiffHistoricalState historicalState = new DiffHistoricalState(to.getVersion());

        SimultaneousExecutor executor = new SimultaneousExecutor();

        for(final TypeDiffInstruction<?> typeInstruction : from.getTypeDiffInstructions()) {
            executor.execute(new Runnable() {
                public void run() {
                    Map<Object, Object> fromTypeState = from.getTypeState(typeInstruction.getTypeIdentifier());
                    Map<Object, Object> toTypeState = to.getTypeState(typeInstruction.getTypeIdentifier());

                    historicalState.addTypeState(typeInstruction, fromTypeState, toTypeState);
                }
            });
        }

        executor.awaitUninterruptibly();

        historicalStates.addFirst(historicalState);
        historicalStateHeaderTags.put(to.getVersion(), new HashMap<String, String>(stateEngine.getHeaderTags()));

        /// trim historical entries beyond desired size.
        if(historicalStates.size() > historySizeToKeep) {
            DiffHistoricalState removedState = historicalStates.removeLast();
            historicalStateHeaderTags.remove(removedState.getVersion());
        }
    }

    /**
     * Return the history of the object identified by the supplied type / key combination.<p/>
     *
     * The returned list will contain one entry for each state in the rolling history retained by this DiffHistoryTracker.
     * The latest entry will be at index 0, and the earliest entry will be the last in the list.  Not all entries in the returned
     * list must reflect a transition; an entry is included in the list whether or not the instance changed for a given state.
     *
     */
    public <T> List<DiffObjectHistoricalTransition<T>> getObjectHistory(String type, Object key) {
        List<DiffObjectHistoricalTransition<T>> states = new ArrayList<DiffObjectHistoricalTransition<T>>(historicalStates.size());
        Map<Object, T> typeState = currentDataState.getTypeState(type);

        /// start with the currently available item (if available)
        T currentItem = typeState.get(key);

        /// and work backwards through history.
        for(DiffHistoricalState state : historicalStates) {

            DiffHistoricalTypeState<Object, T> historicalState = state.getTypeState(type);

            Map<Object, T> diffObjects = historicalState.getDiffObjects();
            Map<Object, T> deletedObjects = historicalState.getDeletedObjects();
            Set<Object> newObjects = historicalState.getNewObjects();

            T previous;

            if(diffObjects.containsKey(key)) {
                previous = diffObjects.get(key);
            } else if(deletedObjects.containsKey(key)) {
                previous = deletedObjects.get(key);
            } else if(newObjects.contains(key)) {
                previous = null;
            } else {
                previous = currentItem;
            }

            /// adding the from -> to objects (whether identical or not) to a list.
            states.add(new DiffObjectHistoricalTransition<T>(state.getVersion(), previous, currentItem));
            currentItem = previous;
        }

        return states;
    }

    /**
     * Returns a list of the historical states, starting with the most recent and ending with the oldest.
     */
    public List<DiffHistoricalState> getHistoricalStates() {
        return Collections.unmodifiableList(historicalStates);
    }

    /**
     * Returns the header tags which were attached to the given version.
     */
    public Map<String, String> getHistoricalStateHeaderTags(String stateVersion) {
        return historicalStateHeaderTags.get(stateVersion);
    }

}
