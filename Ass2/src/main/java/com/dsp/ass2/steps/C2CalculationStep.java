package com.dsp.ass2.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.utils.LLRUtils;
import com.dsp.ass2.models.DecadeWordWord;
import com.dsp.ass2.models.C12C1Value;
import com.dsp.ass2.models.DecadeLLR;
import com.dsp.ass2.models.WordPair;

/**
 * Step 3: Calculate C2, LLR & Filtering
 * 
 * Input: Key=(Decade, w1, w2), Value=(c12, c1)
 * Output: Key=(Decade, LLR), Value=(w1, w2)
 * 
 * Uses Order Inversion pattern:
 * - Emit1: Key=(Decade, w2, *), Value=c12 - for summing C2
 * - Emit2: Key=(Decade, w2, w1), Value=(c12, c1) - for passing data
 * 
 * Mapper emits LongWritable for emit1 and C12C1Value for emit2.
 * Since we need different value types, we use C12C1Value for both
 * and set c1=0 for the count emit (it only needs c12 for summation).
 */
public class C2CalculationStep {

    public static class C2Mapper extends Mapper<DecadeWordWord, C12C1Value, DecadeWordWord, C12C1Value> {
        private DecadeWordWord outKey = new DecadeWordWord();
        private C12C1Value outValue = new C12C1Value();

        @Override
        public void map(DecadeWordWord key, C12C1Value value, Context context)
                throws IOException, InterruptedException {
            int decade = key.getDecade();
            String w1 = key.getW1();
            String w2 = key.getW2();
            long c12 = value.getC12();
            long c1 = value.getC1();

            // Emit 1: Special Key for C2 Aggregation (Decade, w2, *)
            // Use c1=-1 as marker for count records
            outKey.set(decade, w2, "*");
            outValue.set(c12, -1);
            context.write(outKey, outValue);

            // Emit 2: Data Propagation (Decade, w2, w1)
            // Note: we swap w1 and w2 in the key position to group by w2
            outKey.set(decade, w2, w1);
            outValue.set(c12, c1);
            context.write(outKey, outValue);
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

    public static class C2GroupingComparator extends WritableComparator {
        protected C2GroupingComparator() {
            super(DecadeWordWord.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            DecadeWordWord w1 = (DecadeWordWord) a;
            DecadeWordWord w2 = (DecadeWordWord) b;

            int cmp = Integer.compare(w1.getDecade(), w2.getDecade());
            if (cmp != 0)
                return cmp;

            // Grouping by w2 (which we put in getW1() in Mapper)
            return w1.getW1().compareTo(w2.getW1());
        }
    }

    public static class C2Reducer extends Reducer<DecadeWordWord, C12C1Value, DecadeLLR, WordPair> {
        private DecadeLLR outKey = new DecadeLLR();
        private WordPair outValue = new WordPair();

        // Helper class to store buffered data
        private static class DataRecord {
            String w1;
            long c12;
            long c1;

            DataRecord(String w1, long c12, long c1) {
                this.w1 = w1;
                this.c12 = c12;
                this.c1 = c1;
            }
        }

        @Override
        public void reduce(DecadeWordWord key, Iterable<C12C1Value> values, Context context)
                throws IOException, InterruptedException {
            long c2Sum = 0;
            int decade = key.getDecade();
            String w2 = key.getW1(); // This is the w2 of the original bigram
            List<DataRecord> bufferedData = new ArrayList<>();

            // Fetch N for this decade
            long N_decade = context.getConfiguration().getLong("N_" + decade, -1);

            for (C12C1Value val : values) {
                if (val.getC1() == -1) {
                    // This is a count record (c1=-1 is our marker)
                    c2Sum += val.getC12();
                } else {
                    // Buffer data entries to emit after accumulating c2Sum
                    // Note: key.getW2() contains w1 (we swapped them in mapper)
                    bufferedData.add(new DataRecord(key.getW2(), val.getC12(), val.getC1()));
                }
            }

            // Emit all buffered data with the accumulated c2Sum
            for (DataRecord dataVal : bufferedData) {
                String w1 = dataVal.w1;
                long c12 = dataVal.c12;
                long c1 = dataVal.c1;

                // Calculate LLR
                double llr = LLRUtils.calculateLLR(c12, c1, c2Sum, N_decade);

                // Emit: Key=(Decade, LLR), Value=(w1, w2)
                outKey.set(decade, llr);
                outValue.set(w1, w2);
                context.write(outKey, outValue);
            }
        }
    }
}
