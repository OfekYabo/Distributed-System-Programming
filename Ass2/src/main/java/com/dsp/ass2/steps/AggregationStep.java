package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.utils.StopWords;

public class AggregationStep {

    public static class AggregationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private StopWords stopWords;
        private Text outKey = new Text();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void setup(Context context) {
            this.stopWords = new StopWords(context.getConfiguration());
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length < 4)
                return; // Malformed line

            // Assuming format: w1 w2 year count
            // Or: "w1_w2" year count?
            // Google 2-gram format: "word1 word2 year match_count volume_count"

            String w1 = parts[0];
            String w2 = parts[1];
            String yearStr = parts[2];
            String countStr = parts[3];

            // Filter Stop Words
            if (stopWords.isStopWord(w1) || stopWords.isStopWord(w2)) {
                return;
            }

            // Extract Decade
            try {
                int year = Integer.parseInt(yearStr);
                int decade = (year / 10) * 10;
                long count = Long.parseLong(countStr);

                // Emit Key: "decade w1 w2"
                outKey.set(decade + "\t" + w1 + "\t" + w2);
                outValue.set(count);
                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
                // Ignore bad records
            }
        }
    }

    public static class AggregationCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class AggregationReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);

            // Access Decade from Key: "1990\thigh\tschool"
            String[] keyParts = key.toString().split("\t");
            if (keyParts.length >= 1) {
                String decade = keyParts[0];
                // Increment Global Counter for N: "N_1990"
                context.getCounter("Decade_N", "N_" + decade).increment(sum);
            }
        }
    }
}
