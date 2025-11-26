package com.distributed.systems.service;

import com.distributed.systems.ManagerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.Base64;
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
        
        logger.info("Ec2Service initialized for region: {}", config.getAwsRegion());
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
            logger.error("Failed to launch worker instances", e);
            throw new RuntimeException("Failed to launch workers", e);
        }
    }
    
    /**
     * Creates the user data script for worker instances
     */
    private String createWorkerUserDataScript() {
        // This script runs when the instance starts
        // It should start the worker JAR file
        return "#!/bin/bash\n" +
               "cd /home/ec2-user\n" +
               "# Set environment variables\n" +
               "export AWS_REGION=" + config.getAwsRegion() + "\n" +
               "export WORKER_INPUT_QUEUE=" + config.getWorkerInputQueue() + "\n" +
               "export WORKER_OUTPUT_QUEUE=" + config.getWorkerOutputQueue() + "\n" +
               "export S3_BUCKET_NAME=" + config.getS3BucketName() + "\n" +
               "# Run the worker\n" +
               "java -jar worker.jar >> /var/log/worker.log 2>&1 &\n";
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
            logger.error("Failed to terminate instances", e);
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
    
    @Override
    public void close() {
        if (ec2Client != null) {
            ec2Client.close();
            logger.info("Ec2Service closed");
        }
    }
}


