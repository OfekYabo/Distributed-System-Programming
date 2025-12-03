package com.distributed.systems;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Configuration class for Local Application
 * Reads configuration from environment variables with sensible defaults
 */
public class LocalAppConfig {

    // Queue names - Local App communication
    private final String localAppInputQueue;
    private final String localAppOutputQueue;

    // S3 configuration
    private final String s3BucketName;

    // AWS Region
    private final String awsRegion;

    // EC2 configuration for Manager
    private final String managerAmiId;
    private final String managerInstanceType;
    private final String managerIamRole;
    private final String managerSecurityGroup;
    private final String managerKeyName;

    // SQS configuration
    private final int visibilityTimeoutSeconds;
    private final int waitTimeSeconds;
    private final int maxNumberOfMessages;

    // Polling configuration
    private final int pollIntervalSeconds;

    public LocalAppConfig() {
        Dotenv dotenv = Dotenv.load();

        // Queue names - Local App communication
        this.localAppInputQueue = getRequiredEnv(dotenv, "LOCAL_APP_INPUT_QUEUE");
        this.localAppOutputQueue = getRequiredEnv(dotenv, "LOCAL_APP_OUTPUT_QUEUE");

        // S3 bucket
        this.s3BucketName = getRequiredEnv(dotenv, "S3_BUCKET_NAME");

        // AWS Region
        this.awsRegion = getRequiredEnv(dotenv, "AWS_REGION");

        // EC2 configuration for Manager
        this.managerAmiId = getRequiredEnv(dotenv, "MANAGER_AMI_ID");
        this.managerInstanceType = getRequiredEnv(dotenv, "MANAGER_INSTANCE_TYPE");
        this.managerIamRole = getRequiredEnv(dotenv, "MANAGER_IAM_ROLE");
        this.managerSecurityGroup = getRequiredEnv(dotenv, "MANAGER_SECURITY_GROUP");
        this.managerKeyName = getRequiredEnv(dotenv, "MANAGER_KEY_NAME");

        // SQS settings
        this.visibilityTimeoutSeconds = getRequiredIntEnv(dotenv, "VISIBILITY_TIMEOUT_SECONDS");
        this.waitTimeSeconds = getRequiredIntEnv(dotenv, "WAIT_TIME_SECONDS");
        this.maxNumberOfMessages = getRequiredIntEnv(dotenv, "MAX_MESSAGES");

        // Polling configuration
        this.pollIntervalSeconds = getRequiredIntEnv(dotenv, "POLL_INTERVAL_SECONDS");
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

    // Getters
    public String getLocalAppInputQueue() {
        return localAppInputQueue;
    }

    public String getLocalAppOutputQueue() {
        return localAppOutputQueue;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getManagerAmiId() {
        return managerAmiId;
    }

    public String getManagerInstanceType() {
        return managerInstanceType;
    }

    public String getManagerIamRole() {
        return managerIamRole;
    }

    public String getManagerSecurityGroup() {
        return managerSecurityGroup;
    }

    public String getManagerKeyName() {
        return managerKeyName;
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

    public int getPollIntervalSeconds() {
        return pollIntervalSeconds;
    }

    @Override
    public String toString() {
        return "LocalAppConfig{" +
                "localAppInputQueue='" + localAppInputQueue + '\'' +
                ", localAppOutputQueue='" + localAppOutputQueue + '\'' +
                ", s3BucketName='" + s3BucketName + '\'' +
                ", awsRegion='" + awsRegion + '\'' +
                ", managerAmiId='" + managerAmiId + '\'' +
                ", managerInstanceType='" + managerInstanceType + '\'' +
                ", managerIamRole='" + managerIamRole + '\'' +
                '}';
    }
}
