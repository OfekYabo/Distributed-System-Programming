package com.dsp.ass2.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StopWordsTest {

    private File tempStopWordsFile;
    private String stopWordsPath;

    @Before
    public void setUp() throws Exception {
        // Create a temp file
        tempStopWordsFile = File.createTempFile("test-stopwords", ".txt");
        stopWordsPath = tempStopWordsFile.getAbsolutePath();

        // Write content
        try (FileWriter fw = new FileWriter(tempStopWordsFile)) {
            fw.write("is\n");
            fw.write("the\n");
            fw.write("a\n");
            fw.write("in\n");
        }
    }

    @After
    public void tearDown() {
        if (tempStopWordsFile != null && tempStopWordsFile.exists()) {
            tempStopWordsFile.delete();
        }
    }

    @Test
    public void testLoadStopWords_Local() {
        Configuration conf = new Configuration();

        conf.set("stopwords.path", stopWordsPath); // Assumes local path works

        StopWords sw = new StopWords(conf, "eng", "regular");

        assertTrue(sw.isStopWord("is"));
        assertTrue(sw.isStopWord("THE")); // Case sensitivity check
        assertTrue(sw.isStopWord("a"));

        assertFalse(sw.isStopWord("hello"));
        assertFalse(sw.isStopWord("world"));
    }

    @Test
    public void testStopWords_Fallback() {
        Configuration conf = new Configuration();
        conf.set("stopwords.path", "non/existent/path.txt");

        StopWords sw = new StopWords(conf, "eng", "regular");

        assertFalse(sw.isStopWord("is")); // Empty set
    }
}
