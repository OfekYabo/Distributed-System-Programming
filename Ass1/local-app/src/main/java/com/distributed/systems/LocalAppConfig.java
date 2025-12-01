package com.distributed.systems;

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
        // Queue names - Local App communication
        this.localAppInputQueue = getEnvOrDefault("LOCAL_APP_INPUT_QUEUE", "local-app-requests-queue");
        this.localAppOutputQueue = getEnvOrDefault("LOCAL_APP_OUTPUT_QUEUE", "local-app-responses-queue");
        
        // S3 bucket
        this.s3BucketName = getEnvOrDefault("S3_BUCKET_NAME", "ds-assignment-1");
        
        // AWS Region
        this.awsRegion = getEnvOrDefault("AWS_REGION", "us-east-1");
        
        // EC2 configuration for Manager
        // Amazon Linux 2023 AMI (free tier eligible)
        this.managerAmiId = getEnvOrDefault("MANAGER_AMI_ID", "ami-0fa3fe0fa7920f68e");
        this.managerInstanceType = getEnvOrDefault("MANAGER_INSTANCE_TYPE", "t2.micro");
        this.managerIamRole = getEnvOrDefault("MANAGER_IAM_ROLE", "ass1-manager");
        this.managerSecurityGroup = getEnvOrDefault("MANAGER_SECURITY_GROUP", "sg-097d5427acd34969e");
        this.managerKeyName = getEnvOrDefault("MANAGER_KEY_NAME", "ds");
        
        // SQS settings
        this.visibilityTimeoutSeconds = getEnvOrDefaultInt("VISIBILITY_TIMEOUT_SECONDS", 300);
        this.waitTimeSeconds = getEnvOrDefaultInt("WAIT_TIME_SECONDS", 20);
        this.maxNumberOfMessages = getEnvOrDefaultInt("MAX_MESSAGES", 10);
        
        // Polling configuration
        this.pollIntervalSeconds = getEnvOrDefaultInt("POLL_INTERVAL_SECONDS", 5);
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

