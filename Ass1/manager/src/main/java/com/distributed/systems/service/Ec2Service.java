package com.distributed.systems.service;

import com.distributed.systems.ManagerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service for EC2 operations
 * Handles worker instance creation and termination
 */
public class Ec2Service implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(Ec2Service.class);
    
    private static final String WORKER_TAG_KEY = "Role";
    private static final String WORKER_TAG_VALUE = "worker";
    private static final String NAME_TAG_KEY = "Name";
    
    private final Ec2Client ec2Client;
    private final ManagerConfig config;
    
    public Ec2Service(ManagerConfig config) {
        this.config = config;
        
        this.ec2Client = Ec2Client.builder()
                .region(Region.of(config.getAwsRegion()))
                .build();
        
        logger.info("Initialized (region: {})", config.getAwsRegion());
    }
    
    /**
     * Launches new worker instances
     * @param count Number of instances to launch
     * @return List of launched instance IDs
     */
    public List<String> launchWorkers(int count) {
        if (count <= 0) {
            return new ArrayList<>();
        }
        
        logger.info("Launching {} worker instance(s)...", count);
        
        // Create user data script to start the worker
        String userData = createWorkerUserDataScript();
        String encodedUserData = Base64.getEncoder().encodeToString(userData.getBytes());
        
        // Build the run instances request
        RunInstancesRequest.Builder requestBuilder = RunInstancesRequest.builder()
                .imageId(config.getWorkerAmiId())
                .instanceType(InstanceType.fromValue(config.getWorkerInstanceType()))
                .minCount(count)
                .maxCount(count)
                .userData(encodedUserData)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(
                                Tag.builder().key(WORKER_TAG_KEY).value(WORKER_TAG_VALUE).build(),
                                Tag.builder().key(NAME_TAG_KEY).value("TextAnalysis-Worker").build()
                        )
                        .build());
        
        // Add IAM instance profile if specified
        if (config.getWorkerIamRole() != null && !config.getWorkerIamRole().isEmpty()) {
            requestBuilder.iamInstanceProfile(IamInstanceProfileSpecification.builder()
                    .name(config.getWorkerIamRole())
                    .build());
        }
        
        // Add security group if specified
        if (config.getWorkerSecurityGroup() != null && !config.getWorkerSecurityGroup().isEmpty()) {
            requestBuilder.securityGroupIds(config.getWorkerSecurityGroup());
        }
        
        // Add key name if specified
        if (config.getWorkerKeyName() != null && !config.getWorkerKeyName().isEmpty()) {
            requestBuilder.keyName(config.getWorkerKeyName());
        }
        
        try {
            RunInstancesResponse response = ec2Client.runInstances(requestBuilder.build());
            
            List<String> instanceIds = response.instances().stream()
                    .map(Instance::instanceId)
                    .collect(Collectors.toList());
            
            logger.info("Launched {} worker(s): {}", instanceIds.size(), instanceIds);
            return instanceIds;
            
        } catch (Exception e) {
            logger.error("Failed to launch worker instances: {}", e.getMessage());
            throw new RuntimeException("Failed to launch workers", e);
        }
    }
    
    /**
     * Creates the user data script for worker instances
     */
    private String createWorkerUserDataScript() {
        // This script runs when the instance starts
        // It installs Java, downloads worker.jar from S3, and runs it
        return "#!/bin/bash\n" +
               "set -e\n" +
               "exec > /var/log/user-data.log 2>&1\n" +
               "echo 'Starting worker bootstrap...'\n" +
               "cd /home/ec2-user\n" +
               "# Install Java 11 (Amazon Corretto)\n" +
               "echo 'Installing Java...'\n" +
               "dnf install -y java-11-amazon-corretto-headless\n" +
               "# Download worker.jar from S3 (AWS CLI is pre-installed on Amazon Linux 2023)\n" +
               "echo 'Downloading worker.jar from S3...'\n" +
               "aws s3 cp s3://" + config.getS3BucketName() + "/worker.jar /home/ec2-user/worker.jar\n" +
               "# Set environment variables\n" +
               "export AWS_REGION=" + config.getAwsRegion() + "\n" +
               "export WORKER_INPUT_QUEUE=" + config.getWorkerInputQueue() + "\n" +
               "export WORKER_OUTPUT_QUEUE=" + config.getWorkerOutputQueue() + "\n" +
               "export S3_BUCKET_NAME=" + config.getS3BucketName() + "\n" +
               "# Run the worker\n" +
               "echo 'Starting worker...'\n" +
               "java -jar worker.jar >> /var/log/worker.log 2>&1 &\n" +
               "echo 'Worker started successfully'\n";
    }
    
    /**
     * Gets the count of currently running worker instances
     */
    public int getRunningWorkerCount() {
        List<Instance> workers = getRunningWorkers();
        return workers.size();
    }
    
    /**
     * Gets all running worker instances
     */
    public List<Instance> getRunningWorkers() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("tag:" + WORKER_TAG_KEY)
                                .values(WORKER_TAG_VALUE)
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running", "pending")
                                .build()
                )
                .build();
        
        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        
        List<Instance> workers = new ArrayList<>();
        for (Reservation reservation : response.reservations()) {
            workers.addAll(reservation.instances());
        }
        
        logger.debug("Found {} running/pending worker(s)", workers.size());
        return workers;
    }
    
    /**
     * Terminates specific worker instances
     */
    public void terminateInstances(List<String> instanceIds) {
        if (instanceIds == null || instanceIds.isEmpty()) {
            return;
        }
        
        logger.info("Terminating {} instance(s): {}", instanceIds.size(), instanceIds);
        
        TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                .instanceIds(instanceIds)
                .build();
        
        try {
            ec2Client.terminateInstances(request);
            logger.info("Termination request sent for {} instance(s)", instanceIds.size());
        } catch (Exception e) {
            logger.error("Failed to terminate instances: {}", e.getMessage());
            throw new RuntimeException("Failed to terminate instances", e);
        }
    }
    
    /**
     * Terminates all running worker instances
     */
    public void terminateAllWorkers() {
        List<Instance> workers = getRunningWorkers();
        
        if (workers.isEmpty()) {
            logger.info("No workers to terminate");
            return;
        }
        
        List<String> instanceIds = workers.stream()
                .map(Instance::instanceId)
                .collect(Collectors.toList());
        
        terminateInstances(instanceIds);
    }
    
    /**
     * Waits for all workers to terminate
     */
    public void waitForWorkersToTerminate(int timeoutSeconds) {
        logger.info("Waiting for workers to terminate (timeout: {}s)...", timeoutSeconds);
        
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeoutSeconds * 1000L;
        
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            int count = getRunningWorkerCount();
            if (count == 0) {
                logger.info("All workers have terminated");
                return;
            }
            
            logger.debug("Waiting for {} worker(s) to terminate...", count);
            
            try {
                Thread.sleep(5000); // Check every 5 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        
        logger.warn("Timeout waiting for workers to terminate");
    }
    
    /**
     * Gets the current EC2 instance ID from the instance metadata service (IMDSv2)
     * @return The instance ID, or null if not running on EC2 or unable to retrieve
     */
    public String getCurrentInstanceId() {
        try {
            // IMDSv2: First get a token
            URL tokenUrl = URI.create("http://169.254.169.254/latest/api/token").toURL();
            HttpURLConnection tokenConn = (HttpURLConnection) tokenUrl.openConnection();
            tokenConn.setRequestMethod("PUT");
            tokenConn.setRequestProperty("X-aws-ec2-metadata-token-ttl-seconds", "21600");
            tokenConn.setConnectTimeout(1000);
            tokenConn.setReadTimeout(1000);
            
            String token;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(tokenConn.getInputStream()))) {
                token = reader.readLine();
            }
            
            // Use the token to get instance ID
            URL instanceIdUrl = URI.create("http://169.254.169.254/latest/meta-data/instance-id").toURL();
            HttpURLConnection idConn = (HttpURLConnection) instanceIdUrl.openConnection();
            idConn.setRequestMethod("GET");
            idConn.setRequestProperty("X-aws-ec2-metadata-token", token);
            idConn.setConnectTimeout(1000);
            idConn.setReadTimeout(1000);
            
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(idConn.getInputStream()))) {
                String instanceId = reader.readLine();
                logger.debug("Current instance ID: {}", instanceId);
                return instanceId;
            }
            
        } catch (Exception e) {
            logger.warn("Unable to retrieve instance ID from metadata service (not running on EC2?): {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Terminates the current EC2 instance (self-termination)
     * Used by Manager to terminate itself after idle timeout
     */
    public void terminateSelf() {
        String instanceId = getCurrentInstanceId();
        if (instanceId == null) {
            logger.warn("Cannot self-terminate: not running on EC2 or unable to get instance ID");
            return;
        }
        
        logger.info("Self-terminating instance: {}", instanceId);
        terminateInstances(Collections.singletonList(instanceId));
    }
    
    @Override
    public void close() {
        if (ec2Client != null) {
            ec2Client.close();
            logger.info("Closed");
        }
    }
}


