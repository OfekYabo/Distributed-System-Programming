package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.models.WordPair;
import com.dsp.ass2.models.TaggedValue;
import com.dsp.ass2.models.C12Value;

public class C1CalculationStep {

    public static class C1Mapper extends Mapper<WordPair, LongWritable, WordPair, TaggedValue> {
        private WordPair outKey = new WordPair();

        @Override
        public void map(WordPair key, LongWritable value, Context context) throws IOException, InterruptedException {
            int decade = key.getDecade();
            String w1 = key.getW1();
            String w2 = key.getW2();
            long c12 = value.get();

            // Emit 1: Key for C1 aggregation (Decade, w1, *)
            outKey.set(decade, w1, "*");
            context.write(outKey, TaggedValue.count(c12));

            // Emit 2: Data propagation (Decade, w1, w2)
            outKey.set(decade, w1, w2);
            context.write(outKey, TaggedValue.data(w2, c12));
        }
    }

    public static class C1Partitioner extends Partitioner<WordPair, TaggedValue> {
        @Override
        public int getPartition(WordPair key, TaggedValue value, int numPartitions) {
            // Determine partition by "Decade + w1".
            int hash = (key.getDecade() + "\t" + key.getW1()).hashCode();
            return Math.abs(hash) % numPartitions;
        }
    }

    public static class C1GroupingComparator extends WritableComparator {
        protected C1GroupingComparator() {
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

            return w1.getW1().compareTo(w2.getW1());
        }
    }

    public static class C1Reducer extends Reducer<WordPair, TaggedValue, WordPair, C12Value> {
        private C12Value outValue = new C12Value();
        private WordPair outKey = new WordPair();

        @Override
        public void reduce(WordPair key, Iterable<TaggedValue> values, Context context) throws IOException, InterruptedException {
            long c1Sum = 0;
            int decade = key.getDecade();
            String w1 = key.getW1();

            for (TaggedValue val : values) {
                if (val.getType() == TaggedValue.TYPE_COUNT) {
                    c1Sum += val.getC12();
                } else if (val.getType() == TaggedValue.TYPE_DATA) {
                    String w2 = val.getWord();
                    long c12 = val.getC12();

                    // Emit: (Decade, w1, w2) -> (c12, c1Sum)
                    outKey.set(decade, w1, w2);
                    outValue.set(c12, c1Sum);
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
