package com.distributed.systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

/**
 * Handles uploading files to S3
 */
public class S3Uploader {
    
    private static final Logger logger = LoggerFactory.getLogger(S3Uploader.class);
    
    private final S3Client s3Client;
    private final WorkerConfig config;
    
    public S3Uploader(S3Client s3Client, WorkerConfig config) {
        this.s3Client = s3Client;
        this.config = config;
    }
    
    /**
     * Uploads a file to S3 and returns the S3 URL
     * 
     * @param file The file to upload
     * @param originalUrl The original URL of the input file
     * @param parsingMethod The parsing method used
     * @return The S3 URL of the uploaded file
     */
    public String uploadFile(File file, String originalUrl, String parsingMethod) {
        try {
            String s3Key = generateS3Key(originalUrl, parsingMethod);
            
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(config.getS3BucketName())
                    .key(s3Key)
                    .contentType("text/plain")
                    .build();
            
            PutObjectResponse response = s3Client.putObject(putRequest, 
                    RequestBody.fromFile(file.toPath()));
            
            String s3Url = String.format("s3://%s/%s", config.getS3BucketName(), s3Key);
            logger.info("Successfully uploaded file to S3: {}", s3Url);
            
            return s3Url;
        } catch (Exception e) {
            logger.error("Failed to upload file to S3", e);
            throw new RuntimeException("Failed to upload file to S3", e);
        }
    }
    
    /**
     * Generates a unique S3 key for the output file
     * Format: results/{timestamp}/{uuid}/{filename}
     */
    private String generateS3Key(String originalUrl, String parsingMethod) {
        String timestamp = String.valueOf(Instant.now().toEpochMilli());
        String uuid = UUID.randomUUID().toString();
        String filename = extractFilename(originalUrl);
        
        return String.format("results/%s/%s/%s-%s.txt", 
                timestamp, uuid, parsingMethod, filename);
    }
    
    /**
     * Extracts filename from URL
     */
    private String extractFilename(String url) {
        try {
            String[] parts = url.split("/");
            String filename = parts[parts.length - 1];
            
            // Remove extension if present
            if (filename.contains(".")) {
                filename = filename.substring(0, filename.lastIndexOf('.'));
            }
            
            // Remove special characters
            filename = filename.replaceAll("[^a-zA-Z0-9-_]", "_");
            
            return filename.isEmpty() ? "output" : filename;
        } catch (Exception e) {
            return "output";
        }
    }
    
    /**
     * Closes the S3 client
     */
    public void close() {
        if (s3Client != null) {
            s3Client.close();
            logger.info("S3 client closed");
        }
    }
}

