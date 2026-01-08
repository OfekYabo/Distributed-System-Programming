package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class SortStep {

    public static class SortMapper extends Mapper<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // Input from Step 3: Key=(Decade, LLR), Value=(w1, w2)
            // But from SequenceFile input...
            // Step 3 outputted: Key: "Decade \t LLR" (Text), Value: "w1 \t w2" (Text)

            // We just need to forward it. The generic Key/Values are Text.
            // But we want to Sort by LLR numerically.
            // If Key is "1990 \t 100.5", Text sorting converts "100.5" < "20.5"? NO.
            // Standard Text sort is lexicographical: "100" < "20".
            // So we MUST use a Custom Key or manipulate the Text to ensure sorting.
            // OR: Implement a Custom Comparator that parses the Double.

            // Let's use the existing Key ("Decade \t LLR") but use a Comparator that
            // parses.
            context.write(key, value);
        }
    }

    public static class SortPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split("\t");
            if (parts.length >= 1) {
                // Partition by Decade only. All 1990 go to one reducer.
                return Math.abs(parts[0].hashCode()) % numPartitions;
            }
            return 0;
        }
    }

    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            String[] p1 = t1.toString().split("\t");
            String[] p2 = t2.toString().split("\t");

            // Compare Decade (ASC)
            int val = p1[0].compareTo(p2[0]);
            if (val != 0)
                return val;

            // Compare LLR (DESC)
            double llr1 = Double.parseDouble(p1[1]);
            double llr2 = Double.parseDouble(p2[1]);

            if (llr1 > llr2)
                return -1;
            if (llr1 < llr2)
                return 1;
            return 0;
        }
    }

    public static class SortGroupingComparator extends WritableComparator {
        protected SortGroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            String[] p1 = t1.toString().split("\t");
            String[] p2 = t2.toString().split("\t");

            // Group by Decade
            return p1[0].compareTo(p2[0]);
        }
    }

    public static class SortReducer extends Reducer<Text, Text, Text, Text> {
        private int counter = 0;
        private String currentDecade = null;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Since we group by Decade, all Pairs for 1990 come here, sorted by LLR DESC.
            // Wait. reduce() is called ONCE per Group (Decade).
            // So we iterate through ALL values for that Decade.

            counter = 0;
            for (Text val : values) {
                // Check if 100 limit reached
                if (counter < 100) {
                    // Logic: Key has `1990 \t LLR`. Value has `w1 \t w2`.
                    // We want output: `Decade: 1990, Key: w1 w2, Value: LLR`?
                    // Assignment output format: "Decade w1 w2 LLR"?

                    // We need LLR. It's in the Key.
                    // But `key` object updates as we iterate.
                    String[] k = key.toString().split("\t"); // [1990, LLR]

                    Text outKey = new Text("Decade " + k[0] + " " + val.toString());
                    Text outVal = new Text(k[1]);

                    context.write(outKey, outVal);
                    counter++;
                } else {
                    // Start of decade finished?
                    // No, reduce() handles ONE group (Decade). So if we hit 100, we are done for
                    // this decade.
                    // We can break.
                    break;
                }
            }
        }
    }
}
