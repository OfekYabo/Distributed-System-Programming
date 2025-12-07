package com.distributed.systems.shared.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.stream.Collectors;

/**
 * Shared service for S3 operations.
 * Decoupled from specific application configuration.
 */
public class S3Service implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(S3Service.class);

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final String bucketName;

    public S3Service(S3Client s3Client, String bucketName) {
        this(s3Client, null, bucketName);
    }

    public S3Service(S3Client s3Client, S3Presigner s3Presigner, String bucketName) {
        this.s3Client = s3Client;
        this.s3Presigner = s3Presigner;
        this.bucketName = bucketName;
    }

    /**
     * Uploads a local file to S3.
     */
    public String uploadFile(Path localPath, String key) {
        try {
            String content = Files.readString(localPath, StandardCharsets.UTF_8);
            return uploadString(key, content, "text/plain");
        } catch (Exception e) {
            logger.error("Failed to upload file to S3: {} - {}", localPath, e.getMessage());
            throw new RuntimeException("Failed to upload file", e);
        }
    }

    /**
     * Uploads a string content to S3.
     */
    public String uploadString(String key, String content, String contentType) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType(contentType)
                .build();

        s3Client.putObject(request, RequestBody.fromString(content, StandardCharsets.UTF_8));

        String s3Url = String.format("s3://%s/%s", bucketName, key);
        logger.info("Uploaded to {}", s3Url);

        return key;
    }

    /**
     * Downloads a file from S3 and returns its content as a string.
     */
    public String downloadAsString(String key) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        try (var response = s3Client.getObject(request);
                var reader = new BufferedReader(new InputStreamReader(response, StandardCharsets.UTF_8))) {

            String content = reader.lines().collect(Collectors.joining("\n"));
            logger.info("Downloaded s3://{}/{} ({} bytes)", bucketName, key, content.length());
            return content;

        } catch (Exception e) {
            logger.error("Failed to download from S3: s3://{}/{} - {}", bucketName, key, e.getMessage());
            throw new RuntimeException("Failed to download from S3", e);
        }
    }

    /**
     * Downloads a file from S3 and saves it to a local path.
     */
    public void downloadToFile(String key, Path localPath) {
        try {
            String content = downloadAsString(key);
            Files.writeString(localPath, content, StandardCharsets.UTF_8);
            logger.info("Saved to {}", localPath);
        } catch (Exception e) {
            logger.error("Failed to save file: {} - {}", localPath, e.getMessage());
            throw new RuntimeException("Failed to save file", e);
        }
    }

    /**
     * Deletes a file from S3.
     */
    public void deleteFile(String key) {
        try {
            DeleteObjectRequest request = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3Client.deleteObject(request);
            logger.info("Deleted s3://{}/{}", bucketName, key);
        } catch (Exception e) {
            logger.error("Failed to delete file: s3://{}/{} - {}", bucketName, key, e.getMessage());
            throw new RuntimeException("Failed to delete file", e);
        }
    }

    /**
     * Ensures the S3 bucket exists, creating it if necessary.
     */
    public void ensureBucketExists() {
        try {
            HeadBucketRequest request = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.headBucket(request);
            logger.info("Bucket exists: {}", bucketName);

        } catch (NoSuchBucketException e) {
            logger.info("Creating bucket: {}", bucketName);

            CreateBucketRequest createRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();

            s3Client.createBucket(createRequest);
            logger.info("Bucket created: {}", bucketName);
        }
    }

    /**
     * Generates a presigned URL for downloading a file.
     * URL is valid for 1 hour.
     */
    public String generatePresignedUrl(String key) {
        if (s3Presigner == null) {
            throw new IllegalStateException("S3Presigner is not initialized. Use the constructor with S3Presigner.");
        }

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(Duration.ofHours(1))
                    .getObjectRequest(getObjectRequest)
                    .build();

            return s3Presigner.presignGetObject(presignRequest).url().toString();
        } catch (Exception e) {
            logger.error("Failed to generate presigned URL for key: {}", key, e);
            throw new RuntimeException("Failed to generate presigned URL", e);
        }
    }

    public String getBucketName() {
        return bucketName;
    }

    @Override
    public void close() {
        // We do not close the client here as it might be shared or managed externally.
        // If this service owns the client, we should close it.
        // For now, assuming the caller manages the client lifecycle or we can close it
        // if we created it.
        // Given the factory pattern, the caller creates the client.
        // But to be safe and compatible with AutoCloseable usage in existing code:
        // We won't close it to avoid closing a shared client prematurely,
        // OR we can decide that S3Service takes ownership.
        // Let's keep it no-op for now or log.
        logger.debug("S3Service close called (client lifecycle managed externally)");
        if (s3Presigner != null) {
            // We might want to close presigner if we own it, but following the pattern, we
            // assume external management.
            // However, S3Presigner is often created just for this service.
            // Let's leave it open for now or close it if we are sure.
            // Given AwsClientFactory creates it, caller should close it.
        }
    }
}
