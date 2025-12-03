package com.distributed.systems.service;

import com.distributed.systems.LocalAppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Service for EC2 operations
 * Handles Manager instance detection and launching
 */
public class Ec2Service implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Ec2Service.class);

    private static final String MANAGER_TAG_KEY = "Role";
    private static final String MANAGER_TAG_VALUE = "manager";
    private static final String NAME_TAG_KEY = "Name";

    private final Ec2Client ec2Client;
    private final LocalAppConfig config;

    public Ec2Service(LocalAppConfig config) {
        this.config = config;

        this.ec2Client = Ec2Client.builder()
                .region(Region.of(config.getAwsRegion()))
                .build();

        logger.info("EC2 Service initialized (region: {})", config.getAwsRegion());
    }

    /**
     * Checks if a Manager instance is running on EC2
     * 
     * @return true if a Manager is running or pending
     */
    public boolean isManagerRunning() {
        List<Instance> managers = getRunningManagers();
        return !managers.isEmpty();
    }

    /**
     * Gets all running/pending Manager instances
     */
    public List<Instance> getRunningManagers() {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder()
                                .name("tag:" + MANAGER_TAG_KEY)
                                .values(MANAGER_TAG_VALUE)
                                .build(),
                        Filter.builder()
                                .name("instance-state-name")
                                .values("running", "pending")
                                .build())
                .build();

        DescribeInstancesResponse response = ec2Client.describeInstances(request);

        List<Instance> managers = new ArrayList<>();
        for (Reservation reservation : response.reservations()) {
            managers.addAll(reservation.instances());
        }

        logger.debug("Found {} running/pending Manager instance(s)", managers.size());
        return managers;
    }

    /**
     * Ensures a Manager is running on EC2
     * If no Manager is found, launches one and waits for it to start
     * 
     * @return the Manager instance
     */
    public Instance ensureManagerRunning() {
        logger.info("Checking for running Manager instance...");

        List<Instance> managers = getRunningManagers();

        if (!managers.isEmpty()) {
            Instance manager = managers.get(0);
            logger.info("Manager already running: {} ({})",
                    manager.instanceId(), manager.state().nameAsString());
            return manager;
        }

        logger.info("No Manager running - launching new instance...");
        return launchManager();
    }

    /**
     * Launches a new Manager instance on EC2
     * 
     * @return the launched instance
     */
    public Instance launchManager() {
        logger.info("Launching Manager instance...");

        // Create user data script to bootstrap the Manager
        String userData = createManagerUserDataScript();
        String encodedUserData = Base64.getEncoder().encodeToString(userData.getBytes());

        // Build the run instances request
        RunInstancesRequest.Builder requestBuilder = RunInstancesRequest.builder()
                .imageId(config.getManagerAmiId())
                .instanceType(InstanceType.fromValue(config.getManagerInstanceType()))
                .minCount(1)
                .maxCount(1)

                .userData(encodedUserData)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(
                                Tag.builder().key(MANAGER_TAG_KEY).value(MANAGER_TAG_VALUE).build(),
                                Tag.builder().key(NAME_TAG_KEY).value("TextAnalysis-Manager").build())
                        .build());

        // Add IAM instance profile if specified
        if (config.getManagerIamRole() != null && !config.getManagerIamRole().isEmpty()) {
            requestBuilder.iamInstanceProfile(IamInstanceProfileSpecification.builder()
                    .name(config.getManagerIamRole())
                    .build());
        }

        // Add security group if specified
        if (config.getManagerSecurityGroup() != null && !config.getManagerSecurityGroup().isEmpty()) {
            requestBuilder.securityGroupIds(config.getManagerSecurityGroup());
        }

        // Add key name if specified (for SSH access)
        if (config.getManagerKeyName() != null && !config.getManagerKeyName().isEmpty()) {
            requestBuilder.keyName(config.getManagerKeyName());
        }

        try {
            RunInstancesResponse response = ec2Client.runInstances(requestBuilder.build());

            if (response.instances().isEmpty()) {
                throw new RuntimeException("No instances were launched");
            }

            Instance instance = response.instances().get(0);
            logger.info("Manager instance launched: {}", instance.instanceId());

            // Wait for the instance to be running
            waitForInstanceRunning(instance.instanceId());

            return instance;

        } catch (Exception e) {
            logger.error("Failed to launch Manager instance: {}", e.getMessage());
            throw new RuntimeException("Failed to launch Manager", e);
        }
    }

    /**
     * Creates the user data script for the Manager instance
     */
    private String createManagerUserDataScript() {
        // Read the local .env file content
        String envContent;
        try {
            envContent = new String(java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(".env")));
        } catch (java.io.IOException e) {
            logger.warn("Could not read .env file, using empty config: {}", e.getMessage());
            envContent = "";
        }

        // This script runs when the instance starts
        // It installs Java, downloads manager.jar from S3, creates .env file, and runs the manager.
        return "#!/bin/bash\n" +
                "set -e\n" +
                "exec > /var/log/user-data.log 2>&1\n" +
                "echo 'Starting Manager bootstrap...'\n" +
                "cd /home/ec2-user\n" +
                "# Install Java 11 (Amazon Corretto)\n" +
                "echo 'Installing Java...'\n" +
                "dnf install -y java-11-amazon-corretto-headless\n" +
                "# Download manager.jar from S3 (AWS CLI is pre-installed on Amazon Linux 2023)\n" +
                "echo 'Downloading manager.jar from S3...'\n" +
                "aws s3 cp s3://" + config.getS3BucketName() + "/manager.jar /home/ec2-user/manager.jar\n" +
                "# Create .env file with configuration\n" +
                "echo 'Creating .env file...'\n" +
                "cat <<'EOF' > .env\n" +
                envContent + "\n" +
                "EOF\n" +
                "# Run the Manager\n" +
                "echo 'Starting Manager...'\n" +
                "java -jar manager.jar >> /var/log/manager.log 2>&1 &\n" +
                "echo 'Manager started successfully'\n";
    }

    /**
     * Waits for an instance to reach the running state
     */
    private void waitForInstanceRunning(String instanceId) {
        logger.info("Waiting for instance {} to be running...", instanceId);

        int maxAttempts = 60; // 5 minutes max (5 sec intervals)
        int attempt = 0;

        while (attempt < maxAttempts) {
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            DescribeInstancesResponse response = ec2Client.describeInstances(request);

            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (instance.instanceId().equals(instanceId)) {
                        InstanceStateName state = instance.state().name();
                        logger.debug("Instance {} state: {}", instanceId, state);

                        if (state == InstanceStateName.RUNNING) {
                            logger.info("Instance {} is now running", instanceId);
                            // Wait a bit more for the user-data script to start
                            sleep(10000);
                            return;
                        } else if (state == InstanceStateName.TERMINATED ||
                                state == InstanceStateName.SHUTTING_DOWN) {
                            throw new RuntimeException("Instance " + instanceId + " was terminated");
                        }
                    }
                }
            }

            attempt++;
            sleep(5000);
        }

        throw new RuntimeException("Timeout waiting for instance " + instanceId + " to be running");
    }

    /**
     * Sleep helper
     */
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        if (ec2Client != null) {
            ec2Client.close();
            logger.info("EC2 Service closed");
        }
    }
}
