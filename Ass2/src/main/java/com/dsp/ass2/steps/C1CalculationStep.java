package com.dsp.ass2.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.models.DecadeWordWord;
import com.dsp.ass2.models.C12C1Value;
import com.dsp.ass2.models.C12W2Value;

/**
 * Step 2: Calculate C1 (Count of w1)
 * 
 * Input: Key=(Decade, w1, w2), Value=c12
 * Output: Key=(Decade, w1, w2), Value=(c12, c1)
 * 
 * Uses Order Inversion pattern:
 * - Emit1: Key=(Decade, w1, *), Value=(c12, "*") - for summing C1
 * - Emit2: Key=(Decade, w1, w2), Value=(c12, w2) - for passing data with w2
 * marker
 * 
 * Mapper emits C12W2Value for both emit types (c12 + w2 marker).
 * This allows the reducer to distinguish count records from data records.
 * Reducer outputs C12C1Value containing (c12, c1).
 */
public class C1CalculationStep {

    public static class C1Mapper
            extends Mapper<DecadeWordWord, org.apache.hadoop.io.LongWritable, DecadeWordWord, C12W2Value> {
        private DecadeWordWord outKey = new DecadeWordWord();
        private C12W2Value outValue = new C12W2Value();

        @Override
        public void map(DecadeWordWord key, org.apache.hadoop.io.LongWritable value, Context context)
                throws IOException, InterruptedException {
            int decade = key.getDecade();
            String w1 = key.getW1();
            String w2 = key.getW2();
            long c12 = value.get();

            // Emit 1: Key for C1 aggregation (Decade, w1, *)
            // Value includes "*" marker to identify as count record
            outKey.set(decade, w1, "*");
            outValue.set(c12, "*");
            context.write(outKey, outValue);

            // Emit 2: Data propagation (Decade, w1, w2)
            // Value includes actual w2 to identify as data record
            outKey.set(decade, w1, w2);
            outValue.set(c12, w2);
            context.write(outKey, outValue);
        }
    }

    public static class C1Partitioner extends Partitioner<DecadeWordWord, C12W2Value> {
        @Override
        public int getPartition(DecadeWordWord key, C12W2Value value, int numPartitions) {
            // Determine partition by "Decade + w1".
            int hash = (key.getDecade() + "\t" + key.getW1()).hashCode();
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class C1GroupingComparator extends WritableComparator {
        protected C1GroupingComparator() {
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

            return w1.getW1().compareTo(w2.getW1());
        }
    }

    // Helper class to buffer data records
    private static class DataRecord {
        String w2;
        long c12;

        DataRecord(String w2, long c12) {
            this.w2 = w2;
            this.c12 = c12;
        }
    }

    public static class C1Reducer extends Reducer<DecadeWordWord, C12W2Value, DecadeWordWord, C12C1Value> {
        private C12C1Value outValue = new C12C1Value();
        private DecadeWordWord outKey = new DecadeWordWord();

        @Override
        public void reduce(DecadeWordWord key, Iterable<C12W2Value> values, Context context)
                throws IOException, InterruptedException {
            long c1Sum = 0;
            int decade = key.getDecade();
            String w1 = key.getW1();
            List<DataRecord> bufferedData = new ArrayList<>();

            // Iterate through all values
            // Count records have w2="*", data records have actual w2
            for (C12W2Value val : values) {
                String w2 = val.getW2();
                long c12 = val.getC12();

                if (w2.equals("*")) {
                    // This is a count record - accumulate c1
                    c1Sum += c12;
                } else {
                    // Buffer data record to emit after accumulating c1Sum
                    bufferedData.add(new DataRecord(w2, c12));
                }
            }

            // Emit all buffered data records with accumulated c1Sum
            for (DataRecord data : bufferedData) {
                outKey.set(decade, w1, data.w2);
                outValue.set(data.c12, c1Sum);
                context.write(outKey, outValue);
            }
        }
    }
}
