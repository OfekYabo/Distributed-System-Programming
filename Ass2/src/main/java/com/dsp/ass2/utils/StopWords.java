package com.dsp.ass2.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class StopWords {

    private Set<String> stopWords = new HashSet<>();

    public StopWords(String language) {
        String fileName;

        if (language.equalsIgnoreCase("heb")) {
            fileName = "heb-stopwords.txt";
        } else if (language.equalsIgnoreCase("eng")) {
            fileName = "eng-stopwords.txt";
        } else {
            throw new IllegalArgumentException("Unsupported language for StopWords: " + language);
        }

        loadStopWords(fileName);
    }

    private void loadStopWords(String fileName) {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                System.err.println("StopWords file not found in JAR: " + fileName);
                return;
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load stop words from JAR: " + fileName, e);
        }
    }

    public boolean isStopWord(String word) {
        return stopWords.contains(word.toLowerCase());
    }
}
