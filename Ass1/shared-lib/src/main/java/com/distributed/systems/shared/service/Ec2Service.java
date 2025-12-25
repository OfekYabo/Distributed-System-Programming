package com.distributed.systems.shared.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;

import java.util.Base64;
import java.util.List;

/**
 * Shared service for EC2 operations.
 * Decoupled from specific application configuration.
 */
public class Ec2Service implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Ec2Service.class);

    private final Ec2Client ec2Client;

    public Ec2Service(Ec2Client ec2Client) {
        this.ec2Client = ec2Client;
    }

    /**
     * Launches an EC2 instance.
     */
    public Instance launchInstance(String amiId, String instanceType, String userDataScript,
            String iamRole, String securityGroup, String keyName,
            List<Tag> tags, int minCount, int maxCount) {

        logger.info("Launching EC2 instance(s)...");

        String encodedUserData = Base64.getEncoder().encodeToString(userDataScript.getBytes());

        RunInstancesRequest.Builder requestBuilder = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.fromValue(instanceType))
                .minCount(minCount)
                .maxCount(maxCount)
                .userData(encodedUserData);

        if (tags != null && !tags.isEmpty()) {
            requestBuilder.tagSpecifications(TagSpecification.builder()
                    .resourceType(ResourceType.INSTANCE)
                    .tags(tags)
                    .build());
        }

        if (iamRole != null && !iamRole.isEmpty()) {
            requestBuilder.iamInstanceProfile(IamInstanceProfileSpecification.builder()
                    .name(iamRole)
                    .build());
        }

        if (securityGroup != null && !securityGroup.isEmpty()) {
            requestBuilder.securityGroupIds(securityGroup);
        }

        if (keyName != null && !keyName.isEmpty()) {
            requestBuilder.keyName(keyName);
        }

        try {
            RunInstancesResponse response = ec2Client.runInstances(requestBuilder.build());

            if (response.instances().isEmpty()) {
                throw new RuntimeException("No instances were launched");
            }

            Instance instance = response.instances().get(0);
            logger.info("Launched instance: {}", instance.instanceId());
            return instance;

        } catch (Exception e) {
            logger.error("Failed to launch instance: {}", e.getMessage());
            throw new RuntimeException("Failed to launch instance", e);
        }
    }

    /**
     * Checks if an instance with specific tags is running.
     */
    public boolean isInstanceRunning(String tagKey, String tagValue) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:" + tagKey).values(tagValue).build(),
                        Filter.builder().name("instance-state-name").values("running", "pending").build())
                .build();

        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        return response.reservations().stream()
                .anyMatch(r -> !r.instances().isEmpty());
    }

    /**
     * Gets running instances with specific tags.
     */
    public List<Instance> getRunningInstances(String tagKey, String tagValue) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                .filters(
                        Filter.builder().name("tag:" + tagKey).values(tagValue).build(),
                        Filter.builder().name("instance-state-name").values("running", "pending").build())
                .build();

        DescribeInstancesResponse response = ec2Client.describeInstances(request);
        // Flatten list of instances
        return response.reservations().stream()
                .flatMap(r -> r.instances().stream())
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * Terminates an EC2 instance.
     */
    public void terminateInstance(String instanceId) {
        try {
            TerminateInstancesRequest request = TerminateInstancesRequest.builder()
                    .instanceIds(instanceId)
                    .build();

            ec2Client.terminateInstances(request);
            logger.info("Terminated instance: {}", instanceId);

        } catch (Exception e) {
            logger.error("Failed to terminate instance: {}", instanceId, e);
            throw new RuntimeException("Failed to terminate instance", e);
        }
    }

    @Override
    public void close() {
        // Client lifecycle managed externally
        logger.debug("Ec2Service close called (client lifecycle managed externally)");
    }
}
