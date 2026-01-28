package com.dsp.ass2.utils;

public class LLRUtils {

    /**
     * Calculates the Log Likelihood Ratio.
     * Formula: -2 * log(multiplication of likelihoods)
     * But usually simplified to entropy based formula.
     * Assignment specifies: LLR = 2 * (c12 * log(c12) + (c1 - c12) * log(c1 - c12)
     * + ... )
     * Wait, let's copy the formula clearly from assignment description.
     * 
     * H(p) = -p*log(p) - (1-p)*log(1-p)
     * logL(p, k, n) = k*log(p) + (n-k)*log(1-p)
     * LLR = 2 * (logL(p1, k1, n1) + logL(p2, k2, n2) - logL(p, k1+k2, n1+n2))
     * 
     * Using the specific "Dunning" formula for collocations:
     * k1 = c12, n1 = c1
     * k2 = (c2 - c12), n2 = (N - c1)
     * p1 = c12/c1, p2 = (c2-c12)/(N-c1), p = c2/N
     * 
     * Let's implement robustly.
     */
    public static double calculateLLR(long c12, long c1, long c2, long N) {
        if (c12 == 0 || c1 == 0 || c2 == 0 || N == 0)
            return 0.0;

        double k11 = c12;
        double k12 = c2 - c12;
        double k21 = c1 - c12;
        double k22 = N - (c1 + c2 - c12); // N - c1 - c2 + c12

        // Avoid log(0) or division by zero
        if (k11 < 0 || k12 < 0 || k21 < 0 || k22 < 0)
            return 0.0;

        // Common Implementation for Collocations:
        double logL = calcLogL(k11, k12, k21, k22);

        return 2 * logL;
    }

    // Using standard G-Test Log Likelihood
    private static double calcLogL(double k11, double k12, double k21, double k22) {
        double rowSum1 = k11 + k12;
        double rowSum2 = k21 + k22;
        double colSum1 = k11 + k21;
        double colSum2 = k12 + k22;
        double N = rowSum1 + rowSum2;

        double term1 = (k11 > 0) ? k11 * Math.log(k11) : 0;
        double term2 = (k12 > 0) ? k12 * Math.log(k12) : 0;
        double term3 = (k21 > 0) ? k21 * Math.log(k21) : 0;
        double term4 = (k22 > 0) ? k22 * Math.log(k22) : 0;

        double term5 = (rowSum1 > 0) ? rowSum1 * Math.log(rowSum1) : 0;
        double term6 = (rowSum2 > 0) ? rowSum2 * Math.log(rowSum2) : 0;
        double term7 = (colSum1 > 0) ? colSum1 * Math.log(colSum1) : 0;
        double term8 = (colSum2 > 0) ? colSum2 * Math.log(colSum2) : 0;

        double term9 = (N > 0) ? N * Math.log(N) : 0;

        return term1 + term2 + term3 + term4 - term5 - term6 - term7 - term8 + term9;
    }
}
