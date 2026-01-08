package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import com.dsp.ass2.utils.LLRUtils;

public class C2CalculationStep {

    public static class C2Mapper extends Mapper<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // Input from Step 2: (Decade, w1, w2) -> (c12, c1)
            // Value format: "c12 \t c1" (tab separated from Step 2 Reducer)
            // Key format: "Decade \t w1 \t w2"

            String[] keyParts = key.toString().split("\t");
            if (keyParts.length < 3)
                return;

            String decade = keyParts[0];
            String w1 = keyParts[1];
            String w2 = keyParts[2];

            String[] valParts = value.toString().split("\t");
            // Expecting "c12 \t c1"
            if (valParts.length < 2)
                return;

            String c12 = valParts[0];
            String c1 = valParts[1];

            // Emit 1: Special Key for C2 Aggregation (Decade, w2, *)
            outKey.set(decade + "\t" + w2 + "\t*");
            // Value Tag: "C:c12" (We sum c12 to get c2, or simply count occurrences of w2?
            // C2 is total count of w2 as second word.
            // In bigram "A B", B appears c12 times. So Sum(c12) where w2=B is C2. Correct.)
            outValue.set("C:" + c12);
            context.write(outKey, outValue);

            // Emit 2: Data Propagation (Decade, w2, w1)
            // Note: Swapped w1/w2 position in Key to group by w2!
            outKey.set(decade + "\t" + w2 + "\t" + w1);
            // Value Tag: "D:w1:c12:c1" (Need all for LLR)
            outValue.set("D:" + w1 + ":" + c12 + ":" + c1);
            context.write(outKey, outValue);
        }
    }

    public static class C2Partitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split("\t");
            if (parts.length >= 2) {
                // Determine partition by "Decade + w2"
                String partitionKey = parts[0] + "\t" + parts[1];
                return Math.abs(partitionKey.hashCode()) % numPartitions;
            }
            return 0;
        }
    }

    public static class C2GroupingComparator extends WritableComparator {
        protected C2GroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            String[] p1 = t1.toString().split("\t");
            String[] p2 = t2.toString().split("\t");

            // Decade
            int cmp = p1[0].compareTo(p2[0]);
            if (cmp != 0)
                return cmp;

            // w2
            return p1[1].compareTo(p2[1]);
        }
    }

    public static class C2SortComparator extends WritableComparator {
        protected C2SortComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            String[] p1 = t1.toString().split("\t");
            String[] p2 = t2.toString().split("\t");

            // Decade
            int cmp = p1[0].compareTo(p2[0]);
            if (cmp != 0)
                return cmp;

            // w2
            cmp = p1[1].compareTo(p2[1]);
            if (cmp != 0)
                return cmp;

            // w1 (handle *)
            String w1_1 = p1[2];
            String w1_2 = p2[2];

            if (w1_1.equals("*") && w1_2.equals("*"))
                return 0;
            if (w1_1.equals("*"))
                return -1;
            if (w1_2.equals("*"))
                return 1;

            return w1_1.compareTo(w1_2);
        }
    }

    public static class C2Reducer extends Reducer<Text, Text, Text, Text> {
        private long N = 0;
        private Text outKey = new Text();
        private Text outValue = new Text(); // Just "(w1, w2)"

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Retrieve N from configuration (passed by Main)
            // But N is per Decade. We cannot load a single N here if the Reducer handles
            // multiple Decades.
            // However, our Partitioner groups by Decade+w2. So a single reduce() call
            // assumes a single Decade.
            // BUT: keys in the same Reducer might span multiple decades?
            // The Partitioner ensures Decade/w2 are together.
            // So we must fetch N dynamically based on the current decade of the Key.
            // Using a Map or fetching from conf using "N_" + decadeKey.
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long c2Sum = 0;
            String[] keyParts = key.toString().split("\t");
            String decade = keyParts[0];
            String w2 = keyParts[1];

            // Fetch N for this decade
            long N_decade = context.getConfiguration().getLong("N_" + decade, -1);
            if (N_decade <= 0) {
                // Fallback or Error ?
                // Maybe counter wasn't passed correctly.
            }

            for (Text val : values) {
                String s = val.toString();
                int firstColon = s.indexOf(':');
                String type = s.substring(0, firstColon);
                String content = s.substring(firstColon + 1);

                if (type.equals("C")) {
                    c2Sum += Long.parseLong(content);
                } else if (type.equals("D")) {
                    // D:w1:c12:c1
                    // Need to parse complex string
                    String[] data = content.split(":");
                    String w1 = data[0];
                    long c12 = Long.parseLong(data[1]);
                    long c1 = Long.parseLong(data[2]);

                    // Calculate LLR
                    double llr = LLRUtils.calculateLLR(c12, c1, c2Sum, N_decade);

                    // Emit: Key=(Decade, LLR), Value=(w1, w2)
                    // LLR in key for sorting in Step 4
                    outKey.set(decade + "\t" + llr);
                    outValue.set(w1 + "\t" + w2);
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
