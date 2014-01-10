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

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.zeno.diff.DiffInstruction;
import com.netflix.zeno.diff.TypeDiffInstruction;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.testpojos.TypeA;

public class DiffHistoryTrackerUniqueKeyTest extends DiffHistoryAbstractTest {

    private FastBlobStateEngine stateEngine;
    private DiffHistoryTracker diffHistory;
    private int versionCounter;

    @Before
    public void setUp() {
        stateEngine = new FastBlobStateEngine(serializerFactory());
    }

    @Test
    public void testHistory() throws IOException {
        diffHistory = diffHistoryTracker();

        addHistoricalState(1,    null, null, 1);
        addHistoricalState(2,    null,    1, 1);
        addHistoricalState(null,    1,    1, 1);
        addHistoricalState(null,    1,    2, 1);
        addHistoricalState(3,       1,    2, 1);
        addHistoricalState(3,    null,    2, 1);
        addHistoricalState(null, null,    2, 1);

        List<DiffObjectHistoricalTransition<TypeA>> object1History = diffHistory.getObjectHistory("TypeA", Integer.valueOf(1));
        List<DiffObjectHistoricalTransition<TypeA>> object2History = diffHistory.getObjectHistory("TypeA", Integer.valueOf(2));
        List<DiffObjectHistoricalTransition<TypeA>> object3History = diffHistory.getObjectHistory("TypeA", Integer.valueOf(3));
        List<DiffObjectHistoricalTransition<TypeA>> object4History = diffHistory.getObjectHistory("TypeA", Integer.valueOf(4));

        assertDiffObjectHistoricalState(object1History, null, 3, 3, null, null, 2, 1); //1, 2, null, null, 3, 3, null);
        assertDiffObjectHistoricalState(object2History, null, null, 1, 1, 1, null, null);
        assertDiffObjectHistoricalState(object3History, 2, 2, 2, 2, 1, 1, null);
        assertDiffObjectHistoricalState(object4History, 1, 1, 1, 1, 1, 1, 1);
    }

    private void assertDiffObjectHistoricalState(List<DiffObjectHistoricalTransition<TypeA>> list, Integer... expectedHistory) {
        for(int i=0;i<list.size();i++) {
            Integer expectedFrom = expectedHistory[i+1];
            Integer expectedTo = expectedHistory[i];

            TypeA beforeA = list.get(i).getBefore();
            TypeA afterA = list.get(i).getAfter();

            Integer actualFrom = beforeA == null ? null : beforeA.getVal2();
            Integer actualTo = afterA == null ? null : afterA.getVal2();

            Assert.assertEquals(expectedFrom, actualFrom);
            Assert.assertEquals(expectedTo, actualTo);
        }
    }

    private void addHistoricalState(Integer one, Integer two, Integer three, Integer four) throws IOException {
        if(one != null)   stateEngine.add("TypeA", new TypeA(1, one.intValue()));
        if(two != null)   stateEngine.add("TypeA", new TypeA(2, two.intValue()));
        if(three != null) stateEngine.add("TypeA", new TypeA(3, three.intValue()));
        if(four != null)  stateEngine.add("TypeA", new TypeA(4, four.intValue()));

        stateEngine.setLatestVersion(String.valueOf(++versionCounter));

        roundTripObjects(stateEngine);
        diffHistory.addState();
    }

    private DiffHistoryTracker diffHistoryTracker() {
        return new DiffHistoryTracker(10,
                stateEngine,
                new DiffInstruction(
                    new TypeDiffInstruction<TypeA>() {
                        public String getSerializerName() {
                            return "TypeA";
                        }

                        public Object getKey(TypeA object) {
                            return Integer.valueOf(object.getVal1());
                        }
                    }
                )
            );
    }

}
