package com.dsp.ass2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import com.dsp.ass2.steps.AggregationStep;
import com.dsp.ass2.steps.C1CalculationStep;
import com.dsp.ass2.steps.C2CalculationStep;
import com.dsp.ass2.steps.SortStep;

public class Main extends org.apache.hadoop.conf.Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length < 2) {
            System.err.println("Usage: Main <input path> <output base path> [language]");
            return -1;
        }

        String inputPath = args[0];
        String outputBasePath = args[1];
        String language = (args.length > 2) ? args[2] : "eng";
        conf.set("language", language);

        // -------------------------------------------------------------------------
        // Step 1: Aggregation
        // -------------------------------------------------------------------------
        Job step1 = Job.getInstance(conf, "Step 1: Aggregation");
        step1.setJarByClass(Main.class);
        step1.setMapperClass(AggregationStep.AggregationMapper.class);
        step1.setCombinerClass(AggregationStep.AggregationCombiner.class);
        step1.setReducerClass(AggregationStep.AggregationReducer.class);
        step1.setMapOutputKeyClass(Text.class);
        step1.setMapOutputValueClass(LongWritable.class);
        step1.setOutputKeyClass(Text.class);
        step1.setOutputValueClass(LongWritable.class);
        step1.setInputFormatClass(TextInputFormat.class); // Or SequenceFileInputFormat if raw input is such
        step1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(step1, new Path(inputPath));
        FileOutputFormat.setOutputPath(step1, new Path(outputBasePath + "/step1"));

        if (!step1.waitForCompletion(true)) {
            System.err.println("Step 1 failed.");
            return 1;
        }

        // Load Global Counters (N) from Step 1
        Counters counters = step1.getCounters();
        CounterGroup decadeCounters = counters.getGroup("Decade_N");
        for (Counter counter : decadeCounters) {
            conf.setLong("N_" + counter.getName().replace("N_", ""), counter.getValue());
            System.out.println("Loaded N for " + counter.getName() + ": " + counter.getValue());
        }

        // -------------------------------------------------------------------------
        // Step 2: C1 Calculation (Order Inversion)
        // -------------------------------------------------------------------------
        Job step2 = Job.getInstance(conf, "Step 2: C1 Calculation");
        step2.setJarByClass(Main.class);
        step2.setMapperClass(C1CalculationStep.C1Mapper.class);
        step2.setPartitionerClass(C1CalculationStep.C1Partitioner.class);
        step2.setSortComparatorClass(C1CalculationStep.C1SortComparator.class);
        step2.setGroupingComparatorClass(C1CalculationStep.C1GroupingComparator.class);
        step2.setReducerClass(C1CalculationStep.C1Reducer.class);

        step2.setMapOutputKeyClass(Text.class); // Decade, w1, w2 info
        step2.setMapOutputValueClass(Text.class); // Tagged value
        step2.setOutputKeyClass(Text.class);
        step2.setOutputValueClass(Text.class); // (Decade, w1, w2) -> (c12, c1)

        step2.setInputFormatClass(SequenceFileInputFormat.class);
        step2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(step2, new Path(outputBasePath + "/step1"));
        FileOutputFormat.setOutputPath(step2, new Path(outputBasePath + "/step2"));

        if (!step2.waitForCompletion(true)) {
            System.err.println("Step 2 failed.");
            return 1;
        }

        // -------------------------------------------------------------------------
        // Step 3: C2 Calculation & LLR (Order Inversion)
        // -------------------------------------------------------------------------
        Job step3 = Job.getInstance(conf, "Step 3: C2 Calculation & LLR");
        step3.setJarByClass(Main.class);
        step3.setMapperClass(C2CalculationStep.C2Mapper.class);
        step3.setPartitionerClass(C2CalculationStep.C2Partitioner.class);
        step3.setSortComparatorClass(C2CalculationStep.C2SortComparator.class);
        step3.setGroupingComparatorClass(C2CalculationStep.C2GroupingComparator.class);
        step3.setReducerClass(C2CalculationStep.C2Reducer.class);

        step3.setMapOutputKeyClass(Text.class);
        step3.setMapOutputValueClass(Text.class);
        step3.setOutputKeyClass(Text.class); // (Decade, LLR)
        step3.setOutputValueClass(Text.class); // (w1, w2)

        step3.setInputFormatClass(SequenceFileInputFormat.class);
        step3.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(step3, new Path(outputBasePath + "/step2"));
        FileOutputFormat.setOutputPath(step3, new Path(outputBasePath + "/step3"));

        if (!step3.waitForCompletion(true)) {
            System.err.println("Step 3 failed.");
            return 1;
        }

        // -------------------------------------------------------------------------
        // Step 4: Sorting & Output (Top 100)
        // -------------------------------------------------------------------------
        Job step4 = Job.getInstance(conf, "Step 4: Sorting");
        step4.setJarByClass(Main.class);
        step4.setMapperClass(SortStep.SortMapper.class);
        step4.setPartitionerClass(SortStep.SortPartitioner.class);
        step4.setSortComparatorClass(SortStep.SortComparator.class);
        step4.setGroupingComparatorClass(SortStep.SortGroupingComparator.class);
        step4.setReducerClass(SortStep.SortReducer.class);

        step4.setMapOutputKeyClass(Text.class);
        step4.setMapOutputValueClass(Text.class);
        step4.setOutputKeyClass(Text.class);
        step4.setOutputValueClass(Text.class);

        step4.setInputFormatClass(SequenceFileInputFormat.class);
        step4.setOutputFormatClass(TextOutputFormat.class); // Human readable final output

        FileInputFormat.addInputPath(step4, new Path(outputBasePath + "/step3"));
        FileOutputFormat.setOutputPath(step4, new Path(outputBasePath + "/final_output"));

        if (!step4.waitForCompletion(true)) {
            System.err.println("Step 4 failed.");
            return 1;
        }

        System.out.println("All steps completed successfully.");
        return 0;
    }
}
