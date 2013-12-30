package com.netflix.zeno.diff.history;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.netflix.zeno.diff.TypeDiffInstruction;
import com.netflix.zeno.fastblob.FastBlobStateEngine;
import com.netflix.zeno.testpojos.TypeA;

public class DiffHistoryDataStateTest extends DiffHistoryAbstractTest {

    FastBlobStateEngine stateEngine;
    
    @Before
    public void setUp() throws IOException {
        stateEngine = new FastBlobStateEngine(serializerFactory());
        
        stateEngine.add("TypeA", new TypeA(1, 1));
        stateEngine.add("TypeA", new TypeA(2, 2));
        stateEngine.add("TypeA", new TypeA(3, 1));
        stateEngine.add("TypeA", new TypeA(4, 1));
        stateEngine.add("TypeA", new TypeA(5, 3));
        
        roundTripObjects(stateEngine);
    }
    
    @Test
    public void itemsAreOrgainzedByUniqueKey() {
        DiffHistoryDataState dataState = new DiffHistoryDataState(stateEngine, uniqueKeyDiffInstruction);
        
        Map<Integer, TypeA> typeState = dataState.getTypeState("TypeA");
        
        Assert.assertEquals(1, typeState.get(1).getVal2());
        Assert.assertEquals(2, typeState.get(2).getVal2());
        Assert.assertEquals(1, typeState.get(3).getVal2());
        Assert.assertEquals(1, typeState.get(4).getVal2());
        Assert.assertEquals(3, typeState.get(5).getVal2());
    }
    
    @Test
    public void itemsAreGroupedByNonUniqueKey() {
        DiffHistoryDataState dataState = new DiffHistoryDataState(stateEngine, groupedDiffInstruction);
        
        Map<Integer, List<TypeA>> typeState = dataState.getTypeState("TypeAGrouped");
        
        Assert.assertEquals(3, typeState.size());
        
        List<TypeA> list = typeState.get(1);
        
        Assert.assertEquals(3, list.size());
        Assert.assertEquals(1, list.get(0).getVal1());
        Assert.assertEquals(3, list.get(1).getVal1());
        Assert.assertEquals(4, list.get(2).getVal1());
        
        list = typeState.get(2);
        
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(2, list.get(0).getVal1());
        
        list = typeState.get(3);
        
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(5, list.get(0).getVal1());
    }
    
    
    private TypeDiffInstruction<TypeA> uniqueKeyDiffInstruction = new TypeDiffInstruction<TypeA>() {
        public String getSerializerName() {
            return "TypeA";
        }

        @Override
        public Object getKey(TypeA object) {
            return object.getVal1();
        }
    };
    
    private TypeDiffInstruction<TypeA> groupedDiffInstruction = new TypeDiffInstruction<TypeA>() {
        @Override
        public String getSerializerName() {
            return "TypeA";
        }

        @Override
        public Object getKey(TypeA object) {
            return object.getVal2();
        }

        @Override
        public boolean isUniqueKey() {
            return false;
        }

        @Override
        public String getTypeIdentifier() {
            return "TypeAGrouped";
        }
    };


}
