package com.dsp.ass2.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * Utility to convert a standard text file (lines of strings)
 * into a Hadoop SequenceFile (LongWritable key, Text value).
 * 
 * Usage:
 * java -cp ... com.dsp.ass2.utils.CreateSequenceFile <input-text-file>
 * <output-seq-file>
 */
public class CreateSequenceFile {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: CreateSequenceFile <input-text-file> <output-seq-file>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration conf = new Configuration();

        // Setup local filesystem if not explicitly set
        conf.set("fs.defaultFS", "file:///");

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputPath);

        // Delete output file if exists
        if (fs.exists(path)) {
            System.out.println("Output file exists. Deleting: " + outputPath);
            fs.delete(path, false);
        }

        System.out.println("Processing " + inputPath + " -> " + outputPath);

        LongWritable key = new LongWritable();
        Text value = new Text();
        long lineCount = 0;

        try (SequenceFile.Writer writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.file(path),
                SequenceFile.Writer.keyClass(LongWritable.class),
                SequenceFile.Writer.valueClass(Text.class));
                BufferedReader br = new BufferedReader(new FileReader(new File(inputPath)))) {

            String line;
            while ((line = br.readLine()) != null) {
                key.set(lineCount); // Use line number as key (byte offset logic is standard but line num is fine
                                    // here)
                value.set(line);
                writer.append(key, value);
                lineCount++;
            }
        }

        System.out.println("Done. Wrote " + lineCount + " records to " + outputPath);
    }
}
