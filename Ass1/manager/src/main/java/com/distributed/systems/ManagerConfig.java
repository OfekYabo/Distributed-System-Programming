package com.distributed.systems;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Configuration class for Manager
 * Reads configuration from environment variables with sensible defaults
 */
public class ManagerConfig {

    // Queue names - Local App communication
    private final String localAppInputQueue;
    private final String localAppOutputQueue;

    // Queue names - Worker communication
    private final String workerInputQueue;
    private final String workerOutputQueue;

    // S3 configuration
    private final String s3BucketName;

    // AWS Region
    private final String awsRegion;

    // EC2 configuration
    private final String workerAmiId;
    private final String workerInstanceType;
    private final String workerIamRole;
    private final String workerSecurityGroup;
    private final String workerKeyName;
    private final int maxWorkerInstances;

    // SQS configuration
    private final int visibilityTimeoutSeconds;
    private final int waitTimeSeconds;
    private final int maxNumberOfMessages;

    // Manager behavior
    private final int idleTimeoutMinutes;
    private final int scalingIntervalSeconds;

    // S3 Folder Prefixes
    private final String s3ManagerOutputPrefix;

    public ManagerConfig() {
        Dotenv dotenv = Dotenv.load();

        // Queue names - Local App communication
        this.localAppInputQueue = getRequiredEnv(dotenv, "LOCAL_APP_INPUT_QUEUE");
        this.localAppOutputQueue = getRequiredEnv(dotenv, "LOCAL_APP_OUTPUT_QUEUE");

        // Queue names - Worker communication
        this.workerInputQueue = getRequiredEnv(dotenv, "WORKER_INPUT_QUEUE");
        this.workerOutputQueue = getRequiredEnv(dotenv, "WORKER_OUTPUT_QUEUE");

        // S3 bucket
        this.s3BucketName = getRequiredEnv(dotenv, "S3_BUCKET_NAME");

        // AWS Region
        this.awsRegion = getRequiredEnv(dotenv, "AWS_REGION");

        // EC2 configuration
        this.workerAmiId = getRequiredEnv(dotenv, "WORKER_AMI_ID");
        this.workerInstanceType = getRequiredEnv(dotenv, "WORKER_INSTANCE_TYPE");
        this.workerIamRole = getRequiredEnv(dotenv, "WORKER_IAM_ROLE");
        this.workerSecurityGroup = getOptionalEnv(dotenv, "WORKER_SECURITY_GROUP", "");
        this.workerKeyName = getOptionalEnv(dotenv, "WORKER_KEY_NAME", "");
        this.maxWorkerInstances = getRequiredIntEnv(dotenv, "MAX_WORKER_INSTANCES");

        // SQS settings
        this.visibilityTimeoutSeconds = getRequiredIntEnv(dotenv, "VISIBILITY_TIMEOUT_SECONDS");
        this.waitTimeSeconds = getRequiredIntEnv(dotenv, "WAIT_TIME_SECONDS");
        this.maxNumberOfMessages = getRequiredIntEnv(dotenv, "MAX_MESSAGES");
        // Manager behavior
        this.idleTimeoutMinutes = getRequiredIntEnv(dotenv, "IDLE_TIMEOUT_MINUTES");
        this.scalingIntervalSeconds = getRequiredIntEnv(dotenv, "SCALING_INTERVAL_SECONDS");

        // S3 Folder Prefixes
        this.s3ManagerOutputPrefix = getOptionalEnv(dotenv, "S3_MANAGER_OUTPUT_PREFIX", "manager-output");
    }

    private String getRequiredEnv(Dotenv dotenv, String envVar) {
        String value = dotenv.get(envVar);
        if (value == null || value.trim().isEmpty()) {
            throw new RuntimeException("Missing required environment variable: " + envVar);
        }
        return value;
    }

    private String getOptionalEnv(Dotenv dotenv, String envVar, String defaultValue) {
        String value = dotenv.get(envVar);
        return value != null ? value : defaultValue;
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

    public String getWorkerInputQueue() {
        return workerInputQueue;
    }

    public String getWorkerOutputQueue() {
        return workerOutputQueue;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getWorkerAmiId() {
        return workerAmiId;
    }

    public String getWorkerInstanceType() {
        return workerInstanceType;
    }

    public String getWorkerIamRole() {
        return workerIamRole;
    }

    public String getWorkerSecurityGroup() {
        return workerSecurityGroup;
    }

    public String getWorkerKeyName() {
        return workerKeyName;
    }

    public int getMaxWorkerInstances() {
        return maxWorkerInstances;
    }

    public int getVisibilityTimeoutSeconds() {
        return visibilityTimeoutSeconds;
    }

    public int getVisibilityTimeout() {
        return visibilityTimeoutSeconds;
    }

    public int getWaitTimeSeconds() {
        return waitTimeSeconds;
    }

    public int getMaxNumberOfMessages() {
        return maxNumberOfMessages;
    }

    public int getIdleTimeoutMinutes() {
        return idleTimeoutMinutes;
    }

    public int getScalingIntervalSeconds() {
        return scalingIntervalSeconds;
    }

    public String getS3ManagerOutputPrefix() {
        return s3ManagerOutputPrefix;
    }

    @Override
    public String toString() {
        return "ManagerConfig{" +
                "localAppInputQueue='" + localAppInputQueue + '\'' +
                ", localAppOutputQueue='" + localAppOutputQueue + '\'' +
                ", workerInputQueue='" + workerInputQueue + '\'' +
                ", workerOutputQueue='" + workerOutputQueue + '\'' +
                ", s3BucketName='" + s3BucketName + '\'' +
                ", awsRegion='" + awsRegion + '\'' +
                ", workerAmiId='" + workerAmiId + '\'' +
                ", workerInstanceType='" + workerInstanceType + '\'' +
                ", workerIamRole='" + workerIamRole + '\'' +
                ", maxWorkerInstances=" + maxWorkerInstances +
                ", idleTimeoutMinutes=" + idleTimeoutMinutes +
                '}';
    }
}
