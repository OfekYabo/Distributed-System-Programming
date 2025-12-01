package com.distributed.systems.service;

import com.distributed.systems.ManagerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.time.Duration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for S3 operations
 * Handles file download and upload
 */
public class S3Service implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);
    private static final Duration PRESIGNED_URL_EXPIRATION = Duration.ofHours(24);
    
    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final ManagerConfig config;
    
    public S3Service(ManagerConfig config) {
        this.config = config;
        
        Region region = Region.of(config.getAwsRegion());
        
        this.s3Client = S3Client.builder()
                .region(region)
                .build();
        
        this.s3Presigner = S3Presigner.builder()
                .region(region)
                .build();
        
        logger.info("Initialized (region: {})", config.getAwsRegion());
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
            logger.debug("Downloaded s3://{}/{} ({} bytes)", bucket, key, content.length());
            return content;
            
        } catch (Exception e) {
            logger.error("Failed to download from S3: s3://{}/{} - {}", bucket, key, e.getMessage());
            throw new RuntimeException("Failed to download from S3", e);
        }
    }
    
    /**
     * Downloads a file from S3 and returns its content as lines
     */
    public List<String> downloadAsLines(String key) {
        return downloadAsLines(config.getS3BucketName(), key);
    }
    
    /**
     * Downloads a file from S3 and returns its content as lines
     */
    public List<String> downloadAsLines(String bucket, String key) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        
        try (var response = s3Client.getObject(request);
             var reader = new BufferedReader(new InputStreamReader(response, StandardCharsets.UTF_8))) {
            
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    lines.add(line);
                }
            }
            
            logger.debug("Downloaded s3://{}/{} ({} lines)", bucket, key, lines.size());
            return lines;
            
        } catch (Exception e) {
            logger.error("Failed to download from S3: s3://{}/{} - {}", bucket, key, e.getMessage());
            throw new RuntimeException("Failed to download from S3", e);
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
        logger.debug("Uploaded {} ({} bytes)", s3Url, content.length());
        
        return s3Url;
    }
    
    /**
     * Uploads HTML content to S3
     */
    public String uploadHtml(String key, String htmlContent) {
        return uploadString(key, htmlContent, "text/html");
    }
    
    /**
     * Gets the public URL for an S3 object
     */
    public String getPublicUrl(String key) {
        return getPublicUrl(config.getS3BucketName(), key);
    }
    
    /**
     * Gets the public URL for an S3 object
     */
    public String getPublicUrl(String bucket, String key) {
        return String.format("https://%s.s3.%s.amazonaws.com/%s", 
                bucket, config.getAwsRegion(), key);
    }
    
    /**
     * Generates a presigned URL for temporary public access to an S3 object
     */
    public String generatePresignedUrl(String key) {
        return generatePresignedUrl(config.getS3BucketName(), key);
    }
    
    /**
     * Generates a presigned URL for temporary public access to an S3 object
     */
    public String generatePresignedUrl(String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        
        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(PRESIGNED_URL_EXPIRATION)
                .getObjectRequest(getObjectRequest)
                .build();
        
        PresignedGetObjectRequest presignedRequest = s3Presigner.presignGetObject(presignRequest);
        String presignedUrl = presignedRequest.url().toString();
        
        logger.debug("Generated presigned URL for s3://{}/{} (expires in {})", 
                bucket, key, PRESIGNED_URL_EXPIRATION);
        
        return presignedUrl;
    }
    
    /**
     * Generates a presigned URL from an S3 URL (s3://bucket/key format)
     */
    public String generatePresignedUrlFromS3Url(String s3Url) {
        if (s3Url == null || !s3Url.startsWith("s3://")) {
            logger.warn("Invalid S3 URL format: {}", s3Url);
            return s3Url;
        }
        
        // Parse s3://bucket/key format
        String withoutPrefix = s3Url.substring(5); // Remove "s3://"
        int slashIndex = withoutPrefix.indexOf('/');
        if (slashIndex == -1) {
            logger.warn("Invalid S3 URL format (no key): {}", s3Url);
            return s3Url;
        }
        
        String bucket = withoutPrefix.substring(0, slashIndex);
        String key = withoutPrefix.substring(slashIndex + 1);
        
        return generatePresignedUrl(bucket, key);
    }
    
    /**
     * Gets the bucket name from config
     */
    public String getBucketName() {
        return config.getS3BucketName();
    }
    
    @Override
    public void close() {
        if (s3Presigner != null) {
            s3Presigner.close();
        }
        if (s3Client != null) {
            s3Client.close();
            logger.info("Closed");
        }
    }
}


