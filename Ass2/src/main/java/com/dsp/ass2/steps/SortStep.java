package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.models.DecadeLLRKey;
import com.dsp.ass2.models.WordPairValue;

/**
 * Step 4: Sort & Top 100
 * 
 * Input: Key=(Decade, LLR), Value=(w1, w2)
 * Output: Top 100 collocations per decade sorted by LLR descending
 */
public class SortStep {

    public static class SortMapper extends Mapper<DecadeLLRKey, WordPairValue, DecadeLLRKey, WordPairValue> {
        @Override
        public void map(DecadeLLRKey key, WordPairValue value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class SortPartitioner extends Partitioner<DecadeLLRKey, WordPairValue> {
        @Override
        public int getPartition(DecadeLLRKey key, WordPairValue value, int numPartitions) {
            return Math.abs(Integer.hashCode(key.getDecade())) % numPartitions;
        }
    }

    public static class SortGroupingComparator extends WritableComparator {
        protected SortGroupingComparator() {
            super(DecadeLLRKey.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            DecadeLLRKey d1 = (DecadeLLRKey) a;
            DecadeLLRKey d2 = (DecadeLLRKey) b;

            // Group by Decade
            return Integer.compare(d1.getDecade(), d2.getDecade());
        }
    }

    public static class SortReducer extends Reducer<DecadeLLRKey, WordPairValue, Text, Text> {
        @Override
        public void reduce(DecadeLLRKey key, Iterable<WordPairValue> values, Context context)
                throws IOException, InterruptedException {

            int counter = 0;
            for (WordPairValue val : values) {
                if (counter < 100) {
                    // Output format: "Decade decade w1 w2 LLR"
                    Text outKey = new Text("Decade " + key.getDecade() + " " + val.getW1() + " " + val.getW2());
                    Text outVal = new Text(String.valueOf(key.getLlr()));

                    context.write(outKey, outVal);
                    counter++;
                } else {
                    break;
                }
            }
        }
    }
}
