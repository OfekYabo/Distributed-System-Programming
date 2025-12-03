package com.distributed.systems.shared;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * Factory for creating AWS clients with consistent configuration.
 */
public class AwsClientFactory {

    public static S3Client createS3Client(String region) {
        return S3Client.builder()
                .region(Region.of(region))
                .build();
    }

    public static S3Presigner createS3Presigner(String region) {
        return S3Presigner.builder()
                .region(Region.of(region))
                .build();
    }

    public static SqsClient createSqsClient(String region) {
        return SqsClient.builder()
                .region(Region.of(region))
                .build();
    }

    public static Ec2Client createEc2Client(String region) {
        return Ec2Client.builder()
                .region(Region.of(region))
                .build();
    }
}
