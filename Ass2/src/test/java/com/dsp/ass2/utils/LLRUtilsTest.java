package com.dsp.ass2.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class LLRUtilsTest {

    @Test
    public void testCalculateLLR_Zeros() {
        // Should return 0.0 if any input is 0 (simplification for safety)
        assertEquals(0.0, LLRUtils.calculateLLR(0, 100, 100, 1000), 0.0001);
        assertEquals(0.0, LLRUtils.calculateLLR(10, 0, 100, 1000), 0.0001);
        assertEquals(0.0, LLRUtils.calculateLLR(10, 100, 0, 1000), 0.0001);
        assertEquals(0.0, LLRUtils.calculateLLR(10, 100, 100, 0), 0.0001);
    }

    @Test
    public void testCalculateLLR_Calculation() {
        // Example values
        // c12 = 10 (co-occurrences)
        // c1 = 100 (w1 count)
        // c2 = 200 (w2 count)
        // N = 10000 (total bigrams)

        // k11 = 10
        // k12 = 200 - 10 = 190
        // k21 = 100 - 10 = 90
        // k22 = 10000 - 100 - 200 + 10 = 9710

        // This should yield a positive LLR
        double llr = LLRUtils.calculateLLR(10, 100, 200, 10000);
        assertTrue("LLR should be positive", llr > 0);

        // Test symmetry (approximate, since formula depends on c1 vs c2 specific
        // meaning? No, LLR is symmetric for association)
        double llrSym = LLRUtils.calculateLLR(10, 200, 100, 10000);
        assertEquals(llr, llrSym, 0.0001);
    }

    @Test
    public void testCalculateLLR_StrongAssociation() {
        // Case: w1 and w2 always appear together (c12 = c1 = c2)
        // c12=100, c1=100, c2=100, N=10000.
        // k11=100, k12=0, k21=0, k22=9900.
        // This is a very strong association.
        double llr = LLRUtils.calculateLLR(100, 100, 100, 10000);
        assertTrue(llr > 0);
        System.out.println("Strong Association LLR: " + llr);
    }
}
