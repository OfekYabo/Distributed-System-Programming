package com.distributed.systems;

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
        // Queue names - should be set via environment variables
        this.inputQueueName = getEnvOrDefault("WORKER_INPUT_QUEUE", "worker-tasks-queue");
        this.outputQueueName = getEnvOrDefault("WORKER_OUTPUT_QUEUE", "worker-results-queue");
        
        // S3 bucket
        this.s3BucketName = getEnvOrDefault("S3_BUCKET_NAME", "text-analysis-results");
        
        // AWS Region
        this.awsRegion = getEnvOrDefault("AWS_REGION", "us-east-1");
        
        // SQS settings
        this.visibilityTimeoutSeconds = getEnvOrDefaultInt("VISIBILITY_TIMEOUT_SECONDS", 300); // 5 minutes
        this.waitTimeSeconds = getEnvOrDefaultInt("WAIT_TIME_SECONDS", 20); // Long polling
        this.maxNumberOfMessages = getEnvOrDefaultInt("MAX_MESSAGES", 1);
        
        // Processing settings
        this.maxProcessingTimeSeconds = getEnvOrDefaultInt("MAX_PROCESSING_TIME_SECONDS", 240); // 4 minutes
        this.tempDirectory = getEnvOrDefault("TEMP_DIR", "/tmp/worker");
    }
    
    private String getEnvOrDefault(String envVar, String defaultValue) {
        String value = System.getenv(envVar);
        return value != null ? value : defaultValue;
    }
    
    private int getEnvOrDefaultInt(String envVar, int defaultValue) {
        String value = System.getenv(envVar);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
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

