package com.distributed.systems;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Configuration class for Worker
 * Reads configuration from environment variables with sensible defaults
 */
public class WorkerConfig {

    // Queue names
    private final String inputQueueName;
    private final String outputQueueName;

    // S3 configuration
    private final String s3BucketName;

    // AWS Region
    private final String awsRegion;

    // SQS configuration
    private final int visibilityTimeoutSeconds;
    private final int waitTimeSeconds;
    private final int maxNumberOfMessages;

    // Processing configuration
    private final int maxProcessingTimeSeconds;
    private final String tempDirectory;

    public WorkerConfig() {
        Dotenv dotenv = Dotenv.load();

        // Queue names - should be set via environment variables
        this.inputQueueName = getRequiredEnv(dotenv, "WORKER_INPUT_QUEUE");
        this.outputQueueName = getRequiredEnv(dotenv, "WORKER_OUTPUT_QUEUE");

        // S3 bucket
        this.s3BucketName = getRequiredEnv(dotenv, "S3_BUCKET_NAME");

        // AWS Region
        this.awsRegion = getRequiredEnv(dotenv, "AWS_REGION");

        // SQS settings
        this.visibilityTimeoutSeconds = getRequiredIntEnv(dotenv, "VISIBILITY_TIMEOUT_SECONDS");
        this.waitTimeSeconds = getRequiredIntEnv(dotenv, "WAIT_TIME_SECONDS");
        this.maxNumberOfMessages = getRequiredIntEnv(dotenv, "WORKER_MAX_MESSAGES");
        // Processing settings
        this.maxProcessingTimeSeconds = getRequiredIntEnv(dotenv, "MAX_PROCESSING_TIME_SECONDS");
        this.tempDirectory = getRequiredEnv(dotenv, "TEMP_DIR");
    }

    private String getRequiredEnv(Dotenv dotenv, String envVar) {
        String value = dotenv.get(envVar);
        if (value == null || value.trim().isEmpty()) {
            throw new RuntimeException("Missing required environment variable: " + envVar);
        }
        return value;
    }

    private int getRequiredIntEnv(Dotenv dotenv, String envVar) {
        String value = getRequiredEnv(dotenv, envVar);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer for environment variable: " + envVar, e);
        }
    }

    public String getInputQueueName() {
        return inputQueueName;
    }

    public String getOutputQueueName() {
        return outputQueueName;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public int getVisibilityTimeoutSeconds() {
        return visibilityTimeoutSeconds;
    }

    public int getWaitTimeSeconds() {
        return waitTimeSeconds;
    }

    public int getMaxNumberOfMessages() {
        return maxNumberOfMessages;
    }

    public int getMaxProcessingTimeSeconds() {
        return maxProcessingTimeSeconds;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    @Override
    public String toString() {
        return "WorkerConfig{" +
                "inputQueueName='" + inputQueueName + '\'' +
                ", outputQueueName='" + outputQueueName + '\'' +
                ", s3BucketName='" + s3BucketName + '\'' +
                ", awsRegion='" + awsRegion + '\'' +
                ", visibilityTimeoutSeconds=" + visibilityTimeoutSeconds +
                ", waitTimeSeconds=" + waitTimeSeconds +
                ", maxNumberOfMessages=" + maxNumberOfMessages +
                ", maxProcessingTimeSeconds=" + maxProcessingTimeSeconds +
                ", tempDirectory='" + tempDirectory + '\'' +
                '}';
    }
}
