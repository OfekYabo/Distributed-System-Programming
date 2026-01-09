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
import io.github.cdimascio.dotenv.Dotenv;

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

    // Overload for production usage (loads .env automatically)
    public Configuration validateAndConfigure(String[] args) {
        Dotenv dotenv = null;
        try {
            dotenv = Dotenv.load();
        } catch (Exception e) {
            // Ignore if .env missing
        }
        return validateAndConfigure(args, dotenv);
    }

    // Extracted for Testing (accepts injected Dotenv)
    public Configuration validateAndConfigure(String[] args, Dotenv dotenv) {
        Configuration conf = getConf();
        if (conf == null)
            conf = new Configuration();

        // Defaults from .env or null
        // Basic
        String inputPath = (dotenv != null) ? dotenv.get("INPUT_PATH") : null;
        String outputBasePath = (dotenv != null) ? dotenv.get("OUTPUT_PATH") : null;
        String language = (dotenv != null) ? dotenv.get("LANGUAGE") : "eng";
        if (language == null)
            language = "eng";

        // Extended
        String stopWordsStrategy = (dotenv != null) ? dotenv.get("STOP_WORDS_STRATEGY") : "regular";
        if (stopWordsStrategy == null)
            stopWordsStrategy = "regular";
        boolean normalize = (dotenv != null) && Boolean.parseBoolean(dotenv.get("NORMALIZE"));
        String runMode = (dotenv != null) ? dotenv.get("RUN_MODE") : "cloud"; // local, cloud
        String awsProfile = (dotenv != null) ? dotenv.get("AWS_PROFILE") : null;

        // Override with CLI args
        // Usage: <input> <output> [lang] [strategy] [normalize] [runMode]
        if (args.length >= 1)
            inputPath = args[0];
        if (args.length >= 2)
            outputBasePath = args[1];
        if (args.length >= 3)
            language = args[2];
        if (args.length >= 4)
            stopWordsStrategy = args[3];
        if (args.length >= 5)
            normalize = parseBooleanStrict(args[4]);
        if (args.length >= 6)
            runMode = args[5];

        // Validation
        if (inputPath == null || outputBasePath == null) {
            throw new IllegalArgumentException("Missing mandatory parameters: Input Path or Output Path.");
        }

        conf.set("inputPath", inputPath);
        conf.set("outputBasePath", outputBasePath);
        conf.set("language", language);
        conf.set("stopWordsStrategy", stopWordsStrategy);
        conf.setBoolean("normalize", normalize);

        // Handle AWS Profile
        if (awsProfile != null && !awsProfile.isEmpty()) {
            // Set for AWS SDK
            System.setProperty("aws.profile", awsProfile);
            // Set for Hadoop S3A
            conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider");
        }

        if ("local".equalsIgnoreCase(runMode)) {
            conf.set("mapreduce.framework.name", "local");
        }

        return conf;
    }

    // Strict Boolean Parser
    private boolean parseBooleanStrict(String val) {
        if (val == null)
            return false;
        if (val.equalsIgnoreCase("true"))
            return true;
        if (val.equalsIgnoreCase("false"))
            return false;
        throw new IllegalArgumentException("Invalid boolean value: " + val + ". Must be 'true' or 'false'.");
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf;
        try {
            conf = validateAndConfigure(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return -1;
        }

        String inputPath = conf.get("inputPath");
        String outputBasePath = conf.get("outputBasePath");

        setConf(conf);

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
        step1.setInputFormatClass(TextInputFormat.class);
        step1.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(step1, new Path(inputPath));
        FileOutputFormat.setOutputPath(step1, new Path(outputBasePath + "/step1"));

        if (!step1.waitForCompletion(true)) {
            System.err.println("Step 1 failed.");
            return 1;
        }

        // Load Global Counters
        Counters counters = step1.getCounters();
        CounterGroup decadeCounters = counters.getGroup("Decade_N");
        for (Counter counter : decadeCounters) {
            conf.setLong("N_" + counter.getName().replace("N_", ""), counter.getValue());
        }

        // -------------------------------------------------------------------------
        // Step 2: C1 Calculation
        // -------------------------------------------------------------------------
        Job step2 = Job.getInstance(conf, "Step 2: C1 Calculation");
        step2.setJarByClass(Main.class);
        step2.setMapperClass(C1CalculationStep.C1Mapper.class);
        step2.setPartitionerClass(C1CalculationStep.C1Partitioner.class);
        step2.setSortComparatorClass(C1CalculationStep.C1SortComparator.class);
        step2.setGroupingComparatorClass(C1CalculationStep.C1GroupingComparator.class);
        step2.setReducerClass(C1CalculationStep.C1Reducer.class);

        step2.setMapOutputKeyClass(Text.class);
        step2.setMapOutputValueClass(Text.class);
        step2.setOutputKeyClass(Text.class);
        step2.setOutputValueClass(Text.class);

        step2.setInputFormatClass(SequenceFileInputFormat.class);
        step2.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(step2, new Path(outputBasePath + "/step1"));
        FileOutputFormat.setOutputPath(step2, new Path(outputBasePath + "/step2"));

        if (!step2.waitForCompletion(true)) {
            System.err.println("Step 2 failed.");
            return 1;
        }

        // -------------------------------------------------------------------------
        // Step 3: C2 Calculation & LLR
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
        step3.setOutputKeyClass(Text.class);
        step3.setOutputValueClass(Text.class);

        step3.setInputFormatClass(SequenceFileInputFormat.class);
        step3.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(step3, new Path(outputBasePath + "/step2"));
        FileOutputFormat.setOutputPath(step3, new Path(outputBasePath + "/step3"));

        if (!step3.waitForCompletion(true)) {
            System.err.println("Step 3 failed.");
            return 1;
        }

        // -------------------------------------------------------------------------
        // Step 4: Sorting & Output
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
        step4.setOutputFormatClass(TextOutputFormat.class);

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
