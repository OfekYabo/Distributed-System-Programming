package com.distributed.systems.service;

import com.distributed.systems.LocalAppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

/**
 * Service for S3 operations
 * Handles file upload and download for Local Application
 */
public class S3Service implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);
    
    private final S3Client s3Client;
    private final LocalAppConfig config;
    
    public S3Service(LocalAppConfig config) {
        this.config = config;
        
        this.s3Client = S3Client.builder()
                .region(Region.of(config.getAwsRegion()))
                .build();
        
        logger.info("S3 Service initialized (region: {})", config.getAwsRegion());
    }
    
    /**
     * Uploads a local file to S3
     * @param localPath path to the local file
     * @param s3Key the key (path) in S3
     * @return the S3 key
     */
    public String uploadFile(Path localPath, String s3Key) {
        return uploadFile(config.getS3BucketName(), localPath, s3Key);
    }
    
    /**
     * Uploads a local file to S3
     */
    public String uploadFile(String bucket, Path localPath, String s3Key) {
        try {
            String content = Files.readString(localPath, StandardCharsets.UTF_8);
            return uploadString(bucket, s3Key, content, "text/plain");
        } catch (Exception e) {
            logger.error("Failed to upload file to S3: {} - {}", localPath, e.getMessage());
            throw new RuntimeException("Failed to upload file", e);
        }
    }
    
    /**
     * Uploads a string content to S3
     */
    public String uploadString(String key, String content, String contentType) {
        return uploadString(config.getS3BucketName(), key, content, contentType);
    }
    
    /**
     * Uploads a string content to S3
     */
    public String uploadString(String bucket, String key, String content, String contentType) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(contentType)
                .build();
        
        s3Client.putObject(request, RequestBody.fromString(content, StandardCharsets.UTF_8));
        
        String s3Url = String.format("s3://%s/%s", bucket, key);
        logger.info("Uploaded to {}", s3Url);
        
        return key;
    }
    
    /**
     * Downloads a file from S3 and returns its content as a string
     */
    public String downloadAsString(String key) {
        return downloadAsString(config.getS3BucketName(), key);
    }
    
    /**
     * Downloads a file from S3 and returns its content as a string
     */
    public String downloadAsString(String bucket, String key) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        
        try (var response = s3Client.getObject(request);
             var reader = new BufferedReader(new InputStreamReader(response, StandardCharsets.UTF_8))) {
            
            String content = reader.lines().collect(Collectors.joining("\n"));
            logger.info("Downloaded s3://{}/{} ({} bytes)", bucket, key, content.length());
            return content;
            
        } catch (Exception e) {
            logger.error("Failed to download from S3: s3://{}/{} - {}", bucket, key, e.getMessage());
            throw new RuntimeException("Failed to download from S3", e);
        }
    }
    
    /**
     * Downloads a file from S3 and saves it to a local path
     */
    public void downloadToFile(String key, Path localPath) {
        downloadToFile(config.getS3BucketName(), key, localPath);
    }
    
    /**
     * Downloads a file from S3 and saves it to a local path
     */
    public void downloadToFile(String bucket, String key, Path localPath) {
        try {
            String content = downloadAsString(bucket, key);
            Files.writeString(localPath, content, StandardCharsets.UTF_8);
            logger.info("Saved to {}", localPath);
        } catch (Exception e) {
            logger.error("Failed to save file: {} - {}", localPath, e.getMessage());
            throw new RuntimeException("Failed to save file", e);
        }
    }
    
    /**
     * Ensures the S3 bucket exists, creating it if necessary
     */
    public void ensureBucketExists() {
        try {
            HeadBucketRequest request = HeadBucketRequest.builder()
                    .bucket(config.getS3BucketName())
                    .build();
            
            s3Client.headBucket(request);
            logger.info("Bucket exists: {}", config.getS3BucketName());
            
        } catch (NoSuchBucketException e) {
            logger.info("Creating bucket: {}", config.getS3BucketName());
            
            CreateBucketRequest createRequest = CreateBucketRequest.builder()
                    .bucket(config.getS3BucketName())
                    .build();
            
            s3Client.createBucket(createRequest);
            logger.info("Bucket created: {}", config.getS3BucketName());
        }
    }
    
    /**
     * Gets the bucket name from config
     */
    public String getBucketName() {
        return config.getS3BucketName();
    }
    
    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            logger.info("S3 Service closed");
        }
    }
}

