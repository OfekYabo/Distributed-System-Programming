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
        // Use local file system explicitly in URI or just path if code handles it.
        // Our code uses new Path(str) and FileSystem.get(conf).
        // To force local fs, we might need "file:///" prefix depending on Hadoop
        // defaults.
        // But FileSystem.get(conf) defaults to LocalFileSystem if fs.defaultFS is not
        // set.

        conf.set("stopwords.path", stopWordsPath); // Assumes local path works
        // Note: StopWords class uses FileSystem.get(conf). Tests run in local mode
        // usually so this should work.

        StopWords sw = new StopWords(conf);

        assertTrue(sw.isStopWord("is"));
        assertTrue(sw.isStopWord("THE")); // Case sensitivity check
        assertTrue(sw.isStopWord("a"));

        assertFalse(sw.isStopWord("hello"));
        assertFalse(sw.isStopWord("world"));
    }

    @Test
    public void testStopWords_Fallback() {
        // If file doesn't exist, it prints error but doesn't crash (based on
        // implementation).
        Configuration conf = new Configuration();
        conf.set("stopwords.path", "non/existent/path.txt");

        StopWords sw = new StopWords(conf);

        assertFalse(sw.isStopWord("is")); // Empty set
    }
}
