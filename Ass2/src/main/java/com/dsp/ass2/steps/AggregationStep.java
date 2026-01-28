package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import com.dsp.ass2.utils.StopWords;
import com.dsp.ass2.models.DecadeWordWordKey;

public class AggregationStep {

    public static class AggregationMapper extends Mapper<LongWritable, Text, DecadeWordWordKey, LongWritable> {

        private StopWords stopWords;
        private DecadeWordWordKey outKey = new DecadeWordWordKey();
        private LongWritable outValue = new LongWritable();
        private int startDecade = -1;
        private int endDecade = -1;

        @Override
        protected void setup(Context context) {
            // Read configuration for filtering
            String start = context.getConfiguration().get("startDecade");
            if (start != null) {
                startDecade = Integer.parseInt(start);
            }
            String end = context.getConfiguration().get("endDecade");
            if (end != null) {
                endDecade = Integer.parseInt(end);
            }

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
            // OPTIMIZATION: Use StringTokenizer instead of value.toString().split("\\s+")
            // split() compiles a regex pattern every time, which is very slow for billions
            // of records.
            java.util.StringTokenizer tokenizer = new java.util.StringTokenizer(value.toString());

            if (tokenizer.countTokens() < 4)
                return; // Malformed line

            // Google 2-gram format: "word1 word2 year match_count volume_count"
            String rawW1 = tokenizer.nextToken();
            String rawW2 = tokenizer.nextToken();
            String yearStr = tokenizer.nextToken();
            String countStr = tokenizer.nextToken();

            // Sanitize and Validate Tokens
            String w1 = sanitize(rawW1);
            if (w1 == null)
                return; // Fail fast

            String w2 = sanitize(rawW2);
            if (w2 == null)
                return; // Fail fast

            // Filter Stop Words
            if (stopWords.isStopWord(w1) || stopWords.isStopWord(w2)) {
                return;
            }

            // Extract Decade
            try {
                int year = Integer.parseInt(yearStr);
                int decade = (year / 10) * 10;

                // 1. User Configuration Filter (Priority)
                if (startDecade != -1 && decade < startDecade) {
                    return;
                }
                if (endDecade != -1 && decade > endDecade) {
                    return;
                }

                // 2. Global Sanity Check (Broad Range)
                if (year < 1500 || year > 2030) {
                    return;
                }

                long count = Long.parseLong(countStr);

                // Emit Key: DecadeWordWord(decade, w1, w2)
                outKey.set(decade, w1, w2);
                outValue.set(count);
                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
                // Ignore bad records
            }
        }

        /**
         * Sanitizes a token by stripping punctuation and validating it.
         * Returns null if the token should be ignored.
         * OPTIMIZATION: Replaced regex check with manual char check.
         */
        private String sanitize(String token) {
            // Trim non-word characters from start and end (manual loop is faster than regex
            // replaceAll)
            int start = 0;
            int end = token.length() - 1;

            while (start <= end && !Character.isLetterOrDigit(token.charAt(start))) {
                start++;
            }
            while (end >= start && !Character.isLetterOrDigit(token.charAt(end))) {
                end--;
            }

            if (start > end) {
                return null; // Empty after trimming
            }

            String cleaned = token.substring(start, end + 1);

            // Must contain at least one letter (English or Hebrew)
            // OPTIMIZATION: Manual scan instead of matches(".*[a-zA-Z\u0590-\u05FF].*")
            boolean hasLetter = false;
            for (int i = 0; i < cleaned.length(); i++) {
                char c = cleaned.charAt(i);
                if (Character.isLetter(c)) {
                    hasLetter = true;
                    break;
                }
            }
            if (!hasLetter) {
                return null;
            }

            // Filter single letters that are not 'a' or 'i' (or 'A', 'I')
            if (cleaned.length() == 1) {
                char c = cleaned.charAt(0);
                if (c != 'a' && c != 'A' && c != 'i' && c != 'I') {
                    return null;
                }
            }

            return cleaned;
        }
    }

    public static class AggregationCombiner
            extends Reducer<DecadeWordWordKey, LongWritable, DecadeWordWordKey, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(DecadeWordWordKey key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class AggregationReducer
            extends Reducer<DecadeWordWordKey, LongWritable, DecadeWordWordKey, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        public void reduce(DecadeWordWordKey key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
