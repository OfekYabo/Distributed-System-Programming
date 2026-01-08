package com.dsp.ass2.utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StopWords {

    private Set<String> stopWords = new HashSet<>();

    public StopWords(Configuration conf) {
        String stopWordsPath = conf.get("stopwords.path", "eng-stopwords.txt"); // Default fallback
        loadStopWords(conf, stopWordsPath);
    }

    private void loadStopWords(Configuration conf, String pathString) {
        try {
            Path path = new Path(pathString);
            FileSystem fs = FileSystem.get(conf);

            // Check if exists
            if (!fs.exists(path)) {
                System.err.println("StopWords file not found at: " + pathString);
                return;
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load stop words from: " + pathString, e);
        }
    }

    public boolean isStopWord(String word) {
        return stopWords.contains(word.toLowerCase());
    }
}
