package com.dsp.ass2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import io.github.cdimascio.dotenv.Dotenv;

public class MainTest {

    private Main mainDriver;

    @Before
    public void setUp() {
        mainDriver = new Main();
    }

    @Test
    public void testValidateArguments_MinimalArgs_NoEnv() {
        // Simulate no environment variables
        Dotenv mockDotenv = mock(Dotenv.class);
        String[] args = { "s3://input", "s3://output" };

        Configuration conf = mainDriver.validateAndConfigure(args, mockDotenv);

        assertNotNull(conf);
        assertEquals("s3://input", conf.get("inputPath"));
        assertEquals("s3://output", conf.get("outputBasePath"));
        assertEquals("eng", conf.get("language")); // Default
        assertEquals("regular", conf.get("stopWordsStrategy")); // Default
        assertEquals(false, conf.getBoolean("normalize", false)); // Default
    }

    @Test
    public void testValidateArguments_MinimalArgs_WithEnv() {
        // Simulate env vars present
        Dotenv mockDotenv = mock(Dotenv.class);
        when(mockDotenv.get("INPUT_PATH")).thenReturn("env://input");
        when(mockDotenv.get("OUTPUT_PATH")).thenReturn("env://output");
        when(mockDotenv.get("NORMALIZE")).thenReturn("true");

        String[] args = {}; // No CLI args

        Configuration conf = mainDriver.validateAndConfigure(args, mockDotenv);

        assertNotNull(conf);
        assertEquals("env://input", conf.get("inputPath"));
        assertEquals("env://output", conf.get("outputBasePath"));
        assertTrue(conf.getBoolean("normalize", false));
    }

    @Test
    public void testValidateArguments_AllArgs() {
        Dotenv mockDotenv = mock(Dotenv.class);
        String[] args = { "s3://input", "s3://output", "heb", "extended", "true", "local" };

        Configuration conf = mainDriver.validateAndConfigure(args, mockDotenv);

        assertNotNull(conf);
        assertEquals("s3://input", conf.get("inputPath"));
        assertEquals("s3://output", conf.get("outputBasePath"));
        assertEquals("heb", conf.get("language"));
        assertEquals("extended", conf.get("stopWordsStrategy"));
        assertTrue(conf.getBoolean("normalize", false));
        assertEquals("local", conf.get("mapreduce.framework.name"));
    }

    @Test
    public void testValidateArguments_MissingMandatory() {
        Dotenv mockDotenv = mock(Dotenv.class); // Empty env
        String[] args = {}; // Empty args

        try {
            mainDriver.validateAndConfigure(args, mockDotenv); // Should assume no .env values
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Missing mandatory parameters"));
        }
    }

    @Test
    public void testValidateArguments_InvalidBoolean() {
        Dotenv mockDotenv = mock(Dotenv.class);
        String[] args = { "s3://in", "s3://out", "eng", "regular", "not_boolean" };
        try {
            mainDriver.validateAndConfigure(args, mockDotenv);
            fail("Should throw IllegalArgumentException for invalid boolean");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid boolean value"));
        }
    }

    @Test
    public void testAWSProfileConfiguration() {
        Dotenv mockDotenv = mock(Dotenv.class);
        when(mockDotenv.get("AWS_PROFILE")).thenReturn("default"); // The user requirement
        when(mockDotenv.get("INPUT_PATH")).thenReturn("in");
        when(mockDotenv.get("OUTPUT_PATH")).thenReturn("out");

        Configuration conf = mainDriver.validateAndConfigure(new String[] {}, mockDotenv);

        // Verify Hadoop config
        assertEquals("com.amazonaws.auth.profile.ProfileCredentialsProvider",
                conf.get("fs.s3a.aws.credentials.provider"));

        // Verify System Property (Side effect check)
        assertEquals("default", System.getProperty("aws.profile"));
    }
}
