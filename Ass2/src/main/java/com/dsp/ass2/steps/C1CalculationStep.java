package com.dsp.ass2.steps;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class C1CalculationStep {

    public static class C1Mapper extends Mapper<Text, LongWritable, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
            // Key: "1990\thigh\tschool" (From Step 1 Output)
            String[] parts = key.toString().split("\t");
            if (parts.length < 3)
                return;

            String decade = parts[0];
            String w1 = parts[1];
            String w2 = parts[2];
            long c12 = value.get();

            // Emit 1: Key for C1 aggregation "Decade \t w1 \t *"
            outKey.set(decade + "\t" + w1 + "\t*");
            outValue.set("C:" + c12);
            context.write(outKey, outValue);

            // Emit 2: Data propagation "Decade \t w1 \t w2"
            outKey.set(decade + "\t" + w1 + "\t" + w2);
            // Value: "D:w2:c12" (Include w2 just in case key mutability isn't relied upon)
            outValue.set("D:" + w2 + ":" + c12);
            context.write(outKey, outValue);
        }
    }

    public static class C1Partitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] parts = key.toString().split("\t");
            if (parts.length >= 2) {
                // Determine partition by "Decade + w1".
                String partitionKey = parts[0] + "\t" + parts[1];
                return Math.abs(partitionKey.hashCode()) % numPartitions;
            }
            return 0;
        }
    }

    public static class C1SortComparator extends WritableComparator {
        protected C1SortComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            String[] p1 = t1.toString().split("\t");
            String[] p2 = t2.toString().split("\t");

            int cmp = p1[0].compareTo(p2[0]); // Decade
            if (cmp != 0)
                return cmp;

            cmp = p1[1].compareTo(p2[1]); // w1
            if (cmp != 0)
                return cmp;

            String w2_1 = p1[2];
            String w2_2 = p2[2];

            if (w2_1.equals("*") && w2_2.equals("*"))
                return 0;
            if (w2_1.equals("*"))
                return -1; // * first
            if (w2_2.equals("*"))
                return 1;

            return w2_1.compareTo(w2_2);
        }
    }

    public static class C1GroupingComparator extends WritableComparator {
        protected C1GroupingComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            String[] p1 = t1.toString().split("\t");
            String[] p2 = t2.toString().split("\t");

            int cmp = p1[0].compareTo(p2[0]); // Decade
            if (cmp != 0)
                return cmp;

            return p1[1].compareTo(p2[1]); // w1
        }
    }

    public static class C1Reducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long c1Sum = 0;
            // Key is "Decade \t w1 \t ..." (first key in group, usually *)
            String[] keyParts = key.toString().split("\t");
            String decade = keyParts[0];
            String w1 = keyParts[1];

            for (Text val : values) {
                String s = val.toString();
                // Parse "Type:Data"
                int firstColon = s.indexOf(':');
                String type = s.substring(0, firstColon); // "C" or "D"
                String content = s.substring(firstColon + 1);

                if (type.equals("C")) {
                    c1Sum += Long.parseLong(content);
                } else if (type.equals("D")) {
                    // content is "w2:c12"
                    int secondColon = content.lastIndexOf(':'); // Use last index or split?
                    // w2 might contain colon? Unlikely in n-grams but possible.
                    // Let's assume split by last colon.
                    String w2 = content.substring(0, secondColon);
                    String c12Str = content.substring(secondColon + 1);

                    // Emit: (Decade, w1, w2) -> (c12, c1)
                    outKey.set(decade + "\t" + w1 + "\t" + w2);
                    outValue.set(c12Str + "\t" + c1Sum);
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
