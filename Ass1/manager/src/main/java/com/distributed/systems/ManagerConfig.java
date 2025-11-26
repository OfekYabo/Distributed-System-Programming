package com.distributed.systems;

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
    
    public ManagerConfig() {
        // Queue names - Local App communication
        this.localAppInputQueue = getEnvOrDefault("LOCAL_APP_INPUT_QUEUE", "local-app-requests-queue");
        this.localAppOutputQueue = getEnvOrDefault("LOCAL_APP_OUTPUT_QUEUE", "local-app-responses-queue");
        
        // Queue names - Worker communication
        this.workerInputQueue = getEnvOrDefault("WORKER_INPUT_QUEUE", "worker-tasks-queue");
        this.workerOutputQueue = getEnvOrDefault("WORKER_OUTPUT_QUEUE", "worker-results-queue");
        
        // S3 bucket
        this.s3BucketName = getEnvOrDefault("S3_BUCKET_NAME", "text-analysis-resultss");
        
        // AWS Region
        this.awsRegion = getEnvOrDefault("AWS_REGION", "us-east-1");
        
        // EC2 configuration
        this.workerAmiId = getEnvOrDefault("WORKER_AMI_ID", "ami-0fa3fe0fa7920f68e");
        this.workerInstanceType = getEnvOrDefault("WORKER_INSTANCE_TYPE", "t2.micro");
        this.workerIamRole = getEnvOrDefault("WORKER_IAM_ROLE", "ass1-worker");
        this.workerSecurityGroup = getEnvOrDefault("WORKER_SECURITY_GROUP", "");
        this.workerKeyName = getEnvOrDefault("WORKER_KEY_NAME", "");
        this.maxWorkerInstances = getEnvOrDefaultInt("MAX_WORKER_INSTANCES", 19);
        
        // SQS settings
        this.visibilityTimeoutSeconds = getEnvOrDefaultInt("VISIBILITY_TIMEOUT_SECONDS", 300); // 5 minutes
        this.waitTimeSeconds = getEnvOrDefaultInt("WAIT_TIME_SECONDS", 20); // Long polling
        this.maxNumberOfMessages = getEnvOrDefaultInt("MAX_MESSAGES", 10);
        
        // Manager behavior
        this.idleTimeoutMinutes = getEnvOrDefaultInt("IDLE_TIMEOUT_MINUTES", 5);
        this.scalingIntervalSeconds = getEnvOrDefaultInt("SCALING_INTERVAL_SECONDS", 30);
    }
    
    private String getEnvOrDefault(String envVar, String defaultValue) {
        String value = System.getenv(envVar);
        return value != null && !value.isEmpty() ? value : defaultValue;
    }
    
    private int getEnvOrDefaultInt(String envVar, int defaultValue) {
        String value = System.getenv(envVar);
        if (value != null && !value.isEmpty()) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
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


