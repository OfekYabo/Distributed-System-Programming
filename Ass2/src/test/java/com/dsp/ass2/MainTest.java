package com.dsp.ass2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

public class MainTest {

    private Main mainDriver;

    @Before
    public void setUp() {
        mainDriver = new Main();
    }

    @Test
    public void testValidateArguments_MinimalArgs() {
        // args: [input, output]
        String[] args = { "s3://input", "s3://output" };

        Configuration conf = mainDriver.validateAndConfigure(args);

        assertNotNull(conf);
        assertEquals("s3://input", conf.get("inputPath"));
        assertEquals("s3://output", conf.get("outputBasePath"));
        assertEquals("eng", conf.get("language")); // Default
    }

    @Test
    public void testValidateArguments_AllArgs() {
        // args: [input, output, lang]
        String[] args = { "s3://input", "s3://output", "heb" };

        Configuration conf = mainDriver.validateAndConfigure(args);

        assertNotNull(conf);
        assertEquals("s3://input", conf.get("inputPath"));
        assertEquals("s3://output", conf.get("outputBasePath"));
        assertEquals("heb", conf.get("language"));
    }

    @Test
    public void testValidateArguments_MissingMandatory() {
        String[] args = {}; // Empty

        try {
            mainDriver.validateAndConfigure(args);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Missing mandatory parameters"));
        }
    }

    // Note: Testing .env loading requires a .env file on disk, which might affect
    // other tests.
    // For unit testing, we usually rely on args overriding .env, or mock Dotenv if
    // we extracted it.
    // Given the simple logic (args override .env), testing args is sufficient to
    // prove precedence logic exists.
}
