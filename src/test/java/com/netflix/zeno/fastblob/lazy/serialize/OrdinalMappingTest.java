package com.netflix.zeno.fastblob.lazy.serialize;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OrdinalMappingTest {

    private OrdinalMapping ordinalMapping;

    @Before
    public void setUp() {
        ordinalMapping = new OrdinalMapping();
    }

    @Test
    public void mapsOrdinals() {
        for(int i=100;i<200;i++) {
            ordinalMapping.assignNewOrdinal(i);
        }

        for(int i=101;i<200;i+=2) {
            ordinalMapping.releaseOrdinal(i);
        }

        Assert.assertEquals(99, ordinalMapping.assignNewOrdinal(200));
        Assert.assertEquals(97, ordinalMapping.assignNewOrdinal(201));
    }

}
