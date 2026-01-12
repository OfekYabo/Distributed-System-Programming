package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.models.DecadeLLR;
import com.dsp.ass2.models.WordPair;

/**
 * Step 4: Sort & Top 100
 * 
 * Input: Key=(Decade, LLR), Value=(w1, w2)
 * Output: Top 100 collocations per decade sorted by LLR descending
 */
public class SortStep {

    public static class SortMapper extends Mapper<DecadeLLR, WordPair, DecadeLLR, WordPair> {
        @Override
        public void map(DecadeLLR key, WordPair value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class SortPartitioner extends Partitioner<DecadeLLR, WordPair> {
        @Override
        public int getPartition(DecadeLLR key, WordPair value, int numPartitions) {
            return Math.abs(Integer.hashCode(key.getDecade())) % numPartitions;
        }
    }

    public static class SortGroupingComparator extends WritableComparator {
        protected SortGroupingComparator() {
            super(DecadeLLR.class, true);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int compare(WritableComparable a, WritableComparable b) {
            DecadeLLR d1 = (DecadeLLR) a;
            DecadeLLR d2 = (DecadeLLR) b;

            // Group by Decade
            return Integer.compare(d1.getDecade(), d2.getDecade());
        }
    }

    public static class SortReducer extends Reducer<DecadeLLR, WordPair, Text, Text> {
        private int counter = 0;

        @Override
        public void reduce(DecadeLLR key, Iterable<WordPair> values, Context context)
                throws IOException, InterruptedException {

            counter = 0;
            for (WordPair val : values) {
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
