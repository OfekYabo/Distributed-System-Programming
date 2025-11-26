package com.distributed.systems;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Factory class for creating AWS service clients
 * Uses default credential chain (environment variables, IAM roles, etc.)
 */
public class AwsClientFactory {
    
    private final WorkerConfig config;
    
    public AwsClientFactory(WorkerConfig config) {
        this.config = config;
    }
    
    /**
     * Creates an S3 client with default credentials
     * @return S3Client
     */
    public S3Client createS3Client() {
        return S3Client.builder()
                .region(Region.of(config.getAwsRegion()))
                .build();
    }
    
    /**
     * Creates an SQS client with default credentials
     * @return SqsClient
     */
    public SqsClient createSqsClient() {
        return SqsClient.builder()
                .region(Region.of(config.getAwsRegion()))
                .build();
    }
}

