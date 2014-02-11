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

public class DiffHistoryTrackerGroupedKeyTest extends DiffHistoryAbstractTest {

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

        addHistoricalState(2, 1, 1, 1);
        addHistoricalState(2, 1, 1, 3);
        addHistoricalState(2, 2, 1, 3);
        addHistoricalState(1, 1, 1, 3);

        List<DiffObjectHistoricalTransition<List<TypeA>>> object1History = diffHistory.getObjectHistory("TypeA", Integer.valueOf(1));
        List<DiffObjectHistoricalTransition<List<TypeA>>> object2History = diffHistory.getObjectHistory("TypeA", Integer.valueOf(2));
        List<DiffObjectHistoricalTransition<List<TypeA>>> object3History = diffHistory.getObjectHistory("TypeA", Integer.valueOf(3));

        assertDiffObjectHistoricalState(object1History, arr(1, 2, 3), arr(3), arr(2, 3), arr(2, 3, 4));
        assertDiffObjectHistoricalState(object2History, null, arr(1, 2), arr(1), arr(1));
        assertDiffObjectHistoricalState(object3History, arr(4), arr(4), arr(4), null);
    }

    private void assertDiffObjectHistoricalState(List<DiffObjectHistoricalTransition<List<TypeA>>> list, int[]... expectedHistory) {
        for(int i=0;i<list.size();i++) {
            int[] expectedFrom = expectedHistory[i+1];
            int[] expectedTo = expectedHistory[i];

            List<TypeA> beforeA = list.get(i).getBefore();
            List<TypeA> afterA = list.get(i).getAfter();

            int[] actualFrom = toArray(beforeA);
            int[] actualTo = toArray(afterA);

            assertArraysContainSameElements(expectedFrom, actualFrom);
            assertArraysContainSameElements(expectedTo, actualTo);
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
                            return Integer.valueOf(object.getVal2());
                        }

                        @Override
                        public boolean isUniqueKey() {
                            return false;
                        }
                    }
                )
            );
    }

    private void assertArraysContainSameElements(int expected[], int actual[]) {
        if(expected != null && actual != null) {
            for(int ex : expected) {
                assertArrayContains(actual, ex);
            }
        } else if(expected != null || actual != null) {
            Assert.fail();
        }
    }

    private void assertArrayContains(int arr[], int val) {
        for(int x : arr) {
            if(x == val)
                return;
        }
        Assert.fail();
    }

    private int[] arr(int... arr) {
        return arr;
    }

    private int[] toArray(List<TypeA> list) {
        if(list == null)
            return null;

        int arr[] = new int[list.size()];

        for(int i=0;i<arr.length;i++) {
            arr[i] = list.get(i).getVal1();
        }

        return arr;
    }

}
