package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.utils.LLRUtils;
import com.dsp.ass2.models.DecadeWordWord;
import com.dsp.ass2.models.C12C1Value;
import com.dsp.ass2.models.DecadeLLR;
import com.dsp.ass2.models.WordPair;

public class C2CalculationStep {

    public static class C2Mapper extends Mapper<DecadeWordWord, C12C1Value, DecadeWordWord, C12C1Value> {
        private DecadeWordWord outKey = new DecadeWordWord();
        // outValue removed - reusing input value

        @Override
        public void map(DecadeWordWord key, C12C1Value value, Context context)
                throws IOException, InterruptedException {

            // Optimization: Set common fields only once
            // We group by input w2, so it becomes the w1 of outKey
            outKey.setDecade(key.getDecade());
            outKey.setW1(key.getW2());

            // Emit 1: Key for C2 Aggregation (Decade, w2, *)
            outKey.setW2("*");
            context.write(outKey, value);

            // Emit 2: Data Propagation (Decade, w2, w1)
            outKey.setW2(key.getW1());
            context.write(outKey, value);
        }
    }

    public static class C2Partitioner extends Partitioner<DecadeWordWord, C12C1Value> {
        @Override
        public int getPartition(DecadeWordWord key, C12C1Value value, int numPartitions) {
            // Determine partition by "Decade + w2" (which is in key.getW1() now)
            int hash = (key.getDecade() + "\t" + key.getW1()).hashCode();
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class C2Reducer extends Reducer<DecadeWordWord, C12C1Value, DecadeLLR, WordPair> {
        private DecadeLLR outKey = new DecadeLLR();
        private WordPair outValue = new WordPair();
        private long currentC2 = 0;
        private int lastDecade = -1;
        private String lastW2 = "";
        private long currentN = -1;

        @Override
        public void reduce(DecadeWordWord key, Iterable<C12C1Value> values, Context context)
                throws IOException, InterruptedException {

            // Check for group change
            boolean decadeChanged = key.getDecade() != lastDecade;
            boolean wordChanged = !key.getW1().equals(lastW2);

            if (decadeChanged) {
                currentN = context.getConfiguration().getLong("N_" + key.getDecade(), -1);
                lastDecade = key.getDecade();
            }

            if (decadeChanged || wordChanged) {
                currentC2 = 0;
                lastW2 = key.getW1();
            }

            // key.getW2() holds the "second part", which is either * or w1
            if (key.getW2().equals("*")) {
                // This is a count record for C2
                long sum = 0;
                for (C12C1Value val : values) {
                    sum += val.getC12();
                }
                currentC2 = sum;
            } else {
                // Data Record
                String w2 = key.getW1(); // This is w2 (swapped)
                String w1 = key.getW2(); // This is w1 (swapped)

                // use cached currentN
                // We expect exactly one value per (Decade, w2, w1) as Step 2 already aggregated
                // unique pairs
                C12C1Value val = values.iterator().next();

                long c12 = val.getC12();
                long c1 = val.getC1();

                // Calculate LLR
                double llr = LLRUtils.calculateLLR(c12, c1, currentC2, currentN);

                // Emit: Key=(Decade, LLR), Value=(w1, w2)
                outKey.set(lastDecade, llr);
                outValue.set(w1, w2);
                context.write(outKey, outValue);
            }
        }
    }
}
