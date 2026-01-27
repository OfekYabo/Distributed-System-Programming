package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.models.DecadeWordWord;
import com.dsp.ass2.models.C12C1Value;

public class C1CalculationStep {

    public static class C1Mapper
            extends Mapper<DecadeWordWord, LongWritable, DecadeWordWord, LongWritable> {
        private DecadeWordWord outKey = new DecadeWordWord();

        @Override
        public void map(DecadeWordWord key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            // Emit 1: Key for C1 aggregation (Decade, w1, *)
            outKey.set(key.getDecade(), key.getW1(), "*");
            context.write(outKey, value);

            // Emit 2: Data propagation (Decade, w1, w2)
            context.write(key, value);
        }
    }

    public static class C1Partitioner extends Partitioner<DecadeWordWord, LongWritable> {
        @Override
        public int getPartition(DecadeWordWord key, LongWritable value, int numPartitions) {
            // Determine partition by "Decade + w1".
            // Crucial: All (w1,*) and (w1, word) go to SAME partition.
            // Using "\t" separator as in toString() for consistent hashing
            int hash = (key.getDecade() + "\t" + key.getW1()).hashCode();
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class C1Reducer extends Reducer<DecadeWordWord, LongWritable, DecadeWordWord, C12C1Value> {
        private C12C1Value outValue = new C12C1Value();
        private long currentC1 = 0;
        private int lastDecade = -1;
        private String lastW1 = "";

        @Override
        public void reduce(DecadeWordWord key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            // Safety Check: If we switched groupings (e.g. from "high" to "higher"), reset
            // state.
            // Although Partitioner+Sorting guarantees * comes first, this handles
            // boundaries safely.
            if (key.getDecade() != lastDecade || !key.getW1().equals(lastW1)) {
                currentC1 = 0;
                lastDecade = key.getDecade();
                lastW1 = key.getW1();
            }

            if (key.getW2().equals("*")) {
                // Determine C1: Sum all counts in this batch (e.g. Total count of "high")
                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                currentC1 = sum;

                // Increment Global Counter for N: "N_1990" which is sum of all C1s
                context.getCounter("Decade_N", "N_" + key.getDecade()).increment(currentC1);
            } else {
                // Data Record: Emit (c12, currentC1)
                // We sum values here too just in case duplicates exist for (w1, w2) in input
                long c12 = 0;
                for (LongWritable val : values) {
                    c12 += val.get();
                }

                // Use the stateful currentC1 calculated from the "*" reduce call
                outValue.set(c12, currentC1);
                context.write(key, outValue);
            }
        }
    }
}
