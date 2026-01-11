package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.utils.LLRUtils;
import com.dsp.ass2.models.WordPair;
import com.dsp.ass2.models.C12Value;
import com.dsp.ass2.models.TaggedValue;
import com.dsp.ass2.models.DecadeLLR;

public class C2CalculationStep {

    public static class C2Mapper extends Mapper<WordPair, C12Value, WordPair, TaggedValue> {
        private WordPair outKey = new WordPair();

        @Override
        public void map(WordPair key, C12Value value, Context context) throws IOException, InterruptedException {
            int decade = key.getDecade();
            String w1 = key.getW1();
            String w2 = key.getW2();
            long c12 = value.getC12();
            long c1 = value.getC1();

            // Emit 1: Special Key for C2 Aggregation (Decade, w2, *)
            outKey.set(decade, w2, "*");
            context.write(outKey, TaggedValue.count(c12));

            // Emit 2: Data Propagation (Decade, w2, w1)
            outKey.set(decade, w2, w1);
            context.write(outKey, TaggedValue.data(w1, c12, c1));
        }
    }

    public static class C2Partitioner extends Partitioner<WordPair, TaggedValue> {
        @Override
        public int getPartition(WordPair key, TaggedValue value, int numPartitions) {
            // Determine partition by "Decade + w2" (which is in key.getW1() now)
            int hash = (key.getDecade() + "\t" + key.getW1()).hashCode();
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class C2GroupingComparator extends WritableComparator {
        protected C2GroupingComparator() {
            super(WordPair.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            WordPair w1 = (WordPair) a;
            WordPair w2 = (WordPair) b;

            int cmp = Integer.compare(w1.getDecade(), w2.getDecade());
            if (cmp != 0)
                return cmp;

            // Grouping by w2 (which we put in getW1() in Mapper)
            return w1.getW1().compareTo(w2.getW1());
        }
    }

    public static class C2Reducer extends Reducer<WordPair, TaggedValue, DecadeLLR, WordPair> {
        private DecadeLLR outKey = new DecadeLLR();
        private WordPair outValue = new WordPair();

        @Override
        public void reduce(WordPair key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
            long c2Sum = 0;
            int decade = key.getDecade();
            String w2 = key.getW1(); // This is the w2 of the original bigram

            // Fetch N for this decade
            long N_decade = context.getConfiguration().getLong("N_" + decade, -1);

            for (TaggedValue val : values) {
                if (val.getType() == TaggedValue.TYPE_COUNT) {
                    c2Sum += val.getC12();
                } else if (val.getType() == TaggedValue.TYPE_DATA) {
                    String w1 = val.getWord();
                    long c12 = val.getC12();
                    long c1 = val.getC1();

                    // Calculate LLR
                    double llr = LLRUtils.calculateLLR(c12, c1, c2Sum, N_decade);

                    // Emit: Key=(Decade, LLR), Value=(w1, w2)
                    outKey.set(decade, llr);
                    outValue.set(decade, w1, w2);
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
