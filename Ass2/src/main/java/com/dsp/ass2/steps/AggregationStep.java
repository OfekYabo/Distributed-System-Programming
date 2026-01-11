package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.dsp.ass2.utils.StopWords;
import com.dsp.ass2.models.WordPair;

public class AggregationStep {

    public static class AggregationMapper extends Mapper<LongWritable, Text, WordPair, LongWritable> {

        private StopWords stopWords;
        private WordPair outKey = new WordPair();
        private LongWritable outValue = new LongWritable();

        @Override
        protected void setup(Context context) {
            Object split = context.getInputSplit();
            if (!(split instanceof FileSplit)) {
                throw new RuntimeException("Input split is not a FileSplit. Cannot determine language.");
            }

            String path = ((FileSplit) split).getPath().toString().toLowerCase();
            String language;

            if (path.contains("heb")) {
                language = "heb";
            } else if (path.contains("eng")) {
                language = "eng";
            } else {
                throw new RuntimeException("Could not determine language (eng/heb) from input path: " + path);
            }

            this.stopWords = new StopWords(language);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length < 4)
                return; // Malformed line

            // Google 2-gram format: "word1 word2 year match_count volume_count"

            String w1 = parts[0];
            String w2 = parts[1];
            String yearStr = parts[2];
            String countStr = parts[3];

            // Filter Stop Words
            if (w1.isEmpty() || w2.isEmpty() || stopWords.isStopWord(w1) || stopWords.isStopWord(w2)) {
                return;
            }

            // Extract Decade
            try {
                int year = Integer.parseInt(yearStr);
                if (year < 1900 || year > 2010) {
                    return; // Ignore years outside reasonable range for Google Ngrams
                }
                int decade = (year / 10) * 10;
                long count = Long.parseLong(countStr);

                // Emit Key: WordPair(decade, w1, w2)
                outKey.set(decade, w1, w2);
                outValue.set(count);
                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
                // Ignore bad records
            }
        }
    }

    public static class AggregationCombiner extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(WordPair key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class AggregationReducer extends Reducer<WordPair, LongWritable, WordPair, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(WordPair key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);

            // Access Decade from Key
            int decade = key.getDecade();
            // Increment Global Counter for N: "N_1990"
            context.getCounter("Decade_N", "N_" + decade).increment(sum);
        }
    }
}
