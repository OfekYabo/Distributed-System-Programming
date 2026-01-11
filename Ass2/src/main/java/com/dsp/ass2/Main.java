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

import com.dsp.ass2.steps.AggregationStep;
import com.dsp.ass2.steps.C1CalculationStep;
import com.dsp.ass2.steps.C2CalculationStep;
import com.dsp.ass2.steps.SortStep;
import com.dsp.ass2.models.WordPair;
import com.dsp.ass2.models.TaggedValue;
import com.dsp.ass2.models.C12Value;
import com.dsp.ass2.models.DecadeLLR;

public class Main extends org.apache.hadoop.conf.Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Main(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Log received arguments to stderr so they show up in EMR logs
        System.err.println("Received " + args.length + " arguments: " + String.join(", ", args));

        int offset = 0;
        if (args.length > 0 && (args[0].contains("Main") || args[0].endsWith(".jar"))) {
            System.err.println("Detected class name or jar in args[0], shifting offset.");
            offset = 1;
        }

        if (args.length - offset < 2) {
            System.err.println("Usage: Main <input path> <output path>");
            return -1;
        }

        String inputPath = args[0 + offset];
        String baseOutput = args[1 + offset];

        // Generate a unique ID for this run (using timestamp)
        String runId = String.valueOf(System.currentTimeMillis());
        String outputBasePath = baseOutput + (baseOutput.endsWith("/") ? "" : "/") + runId;

        System.err.println("Run ID: " + runId);
        System.err.println("Output Base Path: " + outputBasePath);

        Configuration conf = getConf();
        conf.set("mapreduce.job.counters.max", "1000");

        // Step 1: Aggregation
        Job step1 = Job.getInstance(conf, "Step 1: Aggregation");
        step1.setJarByClass(Main.class);
        step1.setMapperClass(AggregationStep.AggregationMapper.class);
        step1.setCombinerClass(AggregationStep.AggregationCombiner.class);
        step1.setReducerClass(AggregationStep.AggregationReducer.class);
        step1.setMapOutputKeyClass(WordPair.class);
        step1.setMapOutputValueClass(LongWritable.class);
        step1.setOutputKeyClass(WordPair.class);
        step1.setOutputValueClass(LongWritable.class);
        step1.setInputFormatClass(TextInputFormat.class);
        step1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(step1, new Path(inputPath));
        FileOutputFormat.setOutputPath(step1, new Path(outputBasePath + "/step1"));

        if (!step1.waitForCompletion(true)) return 1;

        // Pass Decade_N counters to subsequent jobs
        Counters counters = step1.getCounters();
        CounterGroup decadeCounters = counters.getGroup("Decade_N");
        for (Counter counter : decadeCounters) {
            conf.setLong("N_" + counter.getName().replace("N_", ""), counter.getValue());
        }

        // Step 2: C1 Calculation
        Job step2 = Job.getInstance(conf, "Step 2: C1 Calculation");
        step2.setJarByClass(Main.class);
        step2.setMapperClass(C1CalculationStep.C1Mapper.class);
        step2.setPartitionerClass(C1CalculationStep.C1Partitioner.class);
        step2.setGroupingComparatorClass(C1CalculationStep.C1GroupingComparator.class);
        step2.setReducerClass(C1CalculationStep.C1Reducer.class);
        step2.setMapOutputKeyClass(WordPair.class);
        step2.setMapOutputValueClass(TaggedValue.class);
        step2.setOutputKeyClass(WordPair.class);
        step2.setOutputValueClass(C12Value.class);
        step2.setInputFormatClass(SequenceFileInputFormat.class);
        step2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(step2, new Path(outputBasePath + "/step1"));
        FileOutputFormat.setOutputPath(step2, new Path(outputBasePath + "/step2"));

        if (!step2.waitForCompletion(true)) return 1;

        // Step 3: C2 Calculation & LLR
        Job step3 = Job.getInstance(conf, "Step 3: C2 Calculation & LLR");
        step3.setJarByClass(Main.class);
        step3.setMapperClass(C2CalculationStep.C2Mapper.class);
        step3.setPartitionerClass(C2CalculationStep.C2Partitioner.class);
        step3.setGroupingComparatorClass(C2CalculationStep.C2GroupingComparator.class);
        step3.setReducerClass(C2CalculationStep.C2Reducer.class);
        step3.setMapOutputKeyClass(WordPair.class);
        step3.setMapOutputValueClass(TaggedValue.class);
        step3.setOutputKeyClass(DecadeLLR.class);
        step3.setOutputValueClass(WordPair.class);
        step3.setInputFormatClass(SequenceFileInputFormat.class);
        step3.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(step3, new Path(outputBasePath + "/step2"));
        FileOutputFormat.setOutputPath(step3, new Path(outputBasePath + "/step3"));

        if (!step3.waitForCompletion(true)) return 1;

        // Step 4: Sorting & Output
        Job step4 = Job.getInstance(conf, "Step 4: Sorting");
        step4.setJarByClass(Main.class);
        step4.setMapperClass(SortStep.SortMapper.class);
        step4.setPartitionerClass(SortStep.SortPartitioner.class);
        step4.setGroupingComparatorClass(SortStep.SortGroupingComparator.class);
        step4.setReducerClass(SortStep.SortReducer.class);
        step4.setMapOutputKeyClass(DecadeLLR.class);
        step4.setMapOutputValueClass(WordPair.class);
        step4.setOutputKeyClass(Text.class);
        step4.setOutputValueClass(Text.class);
        step4.setInputFormatClass(SequenceFileInputFormat.class);
        step4.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(step4, new Path(outputBasePath + "/step3"));
        FileOutputFormat.setOutputPath(step4, new Path(outputBasePath + "/final_output"));

        return step4.waitForCompletion(true) ? 0 : 1;
    }
}
