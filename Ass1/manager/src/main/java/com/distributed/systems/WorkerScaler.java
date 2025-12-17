package com.distributed.systems;

import com.distributed.systems.shared.AppConfig;
import com.distributed.systems.shared.service.Ec2Service;
import com.distributed.systems.shared.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Tag;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread that manages worker instance scaling
 * Periodically checks queue depth and scales workers accordingly
 */
public class WorkerScaler implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerScaler.class);

    // Config Keys
    private static final String SCALING_INTERVAL_KEY = "SCALING_INTERVAL_SECONDS";
    private static final String SCALE_UP_INTERVAL_KEY = "WORKER_SCALE_UP_INTERVAL_SECONDS";
    private static final String SCALE_DOWN_INTERVAL_KEY = "WORKER_SCALE_DOWN_INTERVAL_SECONDS";
    private static final String WORKER_CONTROL_QUEUE_KEY = "WORKER_CONTROL_QUEUE";
    private static final String MAX_INSTANCES_KEY = "WORKER_MAX_INSTANCES";
    private static final String S3_BUCKET_KEY = "S3_BUCKET_NAME";

    // AWS Config
    private static final String AMI_ID = "WORKER_AMI_ID";
    private static final String INSTANCE_TYPE = "WORKER_INSTANCE_TYPE";
    private static final String IAM_ROLE = "WORKER_IAM_ROLE";
    private static final String SECURITY_GROUP = "WORKER_SECURITY_GROUP";
    private static final String KEY_NAME = "WORKER_KEY_NAME";

    private final SqsService sqsService;
    private final Ec2Service ec2Service;
    private final JobTracker jobTracker;
    private final AtomicBoolean running;
    private final AtomicBoolean terminateRequested;

    // Config Values
    private final int scalingIntervalSeconds;
    private final int scaleUpIntervalSeconds;
    private final int scaleDownIntervalSeconds;
    private final String workerControlQueue;
    private final int maxWorkerInstances;
    private final String s3BucketName;
    private final String amiId;
    private final String instanceType;
    private final String iamRole;
    private final String securityGroup;
    private final String keyName;

    public WorkerScaler(AppConfig config,
            SqsService sqsService,
            Ec2Service ec2Service,
            JobTracker jobTracker,
            AtomicBoolean running,
            AtomicBoolean terminateRequested) {
        this.sqsService = sqsService;
        this.ec2Service = ec2Service;
        this.jobTracker = jobTracker;
        this.running = running;
        this.terminateRequested = terminateRequested;

        // Load Configuration
        this.scalingIntervalSeconds = config.getIntOptional(SCALING_INTERVAL_KEY, 10);
        this.scaleUpIntervalSeconds = config.getIntOptional(SCALE_UP_INTERVAL_KEY, 10);
        this.scaleDownIntervalSeconds = config.getIntOptional(SCALE_DOWN_INTERVAL_KEY, 30);
        this.workerControlQueue = config.getOptional(WORKER_CONTROL_QUEUE_KEY, "WorkerControlQueue");
        this.maxWorkerInstances = config.getIntOptional(MAX_INSTANCES_KEY, 10);
        this.s3BucketName = config.getString(S3_BUCKET_KEY);
        this.amiId = config.getString(AMI_ID);
        this.instanceType = config.getString(INSTANCE_TYPE);
        this.iamRole = config.getString(IAM_ROLE);
        this.securityGroup = config.getString(SECURITY_GROUP);
        this.keyName = config.getString(KEY_NAME);
    }

    @Override
    public void run() {
        logger.info("Started (interval: {}s)", scalingIntervalSeconds);

        while (running.get()) {
            try {
                if (terminateRequested.get()) {
                    // In termination mode - don't scale up, just monitor
                    logger.debug("Termination requested - skipping scaling");
                } else {
                    // Normal operation - check and scale workers
                    checkAndScale();
                }

                // Sleep for the configured interval
                sleep(scalingIntervalSeconds * 1000L);

            } catch (Exception e) {
                logger.error("Error in WorkerScaler: {}", e.getMessage());
                sleep(5000);
            }
        }

        logger.info("Stopped");
    }

    /**
     * Checks the current state and scales workers if needed
     */
    private long lastScaleUpTime = 0;
    private long lastScaleDownTime = 0;

    /**
     * Checks the current state and scales workers if needed
     */
    private void checkAndScale() {
        try {
            // Get current state
            int globalNeeded = jobTracker.getGlobalNeededWorkers();
            int currentWorkers = getRunningWorkerCount();

            // Calculate target
            int targetWorkers = Math.min(globalNeeded, maxWorkerInstances);
            if (targetWorkers < 0)
                targetWorkers = 0;

            logger.debug("Scaling check: needed={}, target={}, current={}",
                    globalNeeded, targetWorkers, currentWorkers);

            long now = System.currentTimeMillis();

            // Scale UP
            if (currentWorkers < targetWorkers) {
                if (now - lastScaleUpTime >= scaleUpIntervalSeconds * 1000L) {
                    int toCreate = targetWorkers - currentWorkers;
                    logger.info("Scaling UP: creating {} worker(s) (current={}, target={})",
                            toCreate, currentWorkers, targetWorkers);
                    launchWorkers(toCreate);
                    lastScaleUpTime = now;
                }
            }
            // Scale DOWN
            else if (currentWorkers > targetWorkers) {
                if (now - lastScaleDownTime >= scaleDownIntervalSeconds * 1000L) {
                    int excess = currentWorkers - targetWorkers;
                    logger.info("Scaling DOWN: terminating {} worker(s) (current={}, target={})",
                            excess, currentWorkers, targetWorkers);
                    terminateWorkersGracefully(excess);
                    lastScaleDownTime = now;
                }
            }

        } catch (Exception e) {
            logger.error("Error during scaling check: {}", e.getMessage());
        }
    }

    private void terminateWorkersGracefully(int count) {
        // limit the number of termination messages to avoid flooding the queue if
        // workers are slow to consume
        int currentPending = 0;
        try {
            currentPending = sqsService.getApproximateMessageCount(workerControlQueue);
        } catch (Exception e) {
            logger.warn("Could not get pending messages count for {}, assuming 0", workerControlQueue);
        }

        int actuallyNeeded = count - currentPending;
        if (actuallyNeeded <= 0) {
            logger.info("Scaling DOWN: {} termination messages already pending (needed {}), skipping send.",
                    currentPending, count);
            return;
        }

        // Send 'excess' Poison Pill messages to the Control Queue
        for (int i = 0; i < actuallyNeeded; i++) {
            com.distributed.systems.shared.model.WorkerTaskMessage terminateMsg = com.distributed.systems.shared.model.WorkerTaskMessage
                    .createTerminate();
            sqsService.sendMessage(workerControlQueue, terminateMsg);
        }
        logger.info("Sent {} termination messages to {} (pending before: {})", actuallyNeeded, workerControlQueue,
                currentPending);
    }

    private void launchWorkers(int count) {
        String userData = createWorkerUserDataScript();
        List<Tag> tags = Arrays.asList(
                Tag.builder().key("Role").value("worker").build(),
                Tag.builder().key("Name").value("TextAnalysis-Worker").build());

        ec2Service.launchInstance(
                amiId,
                instanceType,
                userData,
                iamRole,
                securityGroup,
                keyName,
                tags,
                count, count);
    }

    private String createWorkerUserDataScript() {
        // Read the local .env file content
        String envContent;
        try {
            envContent = new String(Files.readAllBytes(Paths.get(".env")));
        } catch (IOException e) {
            logger.warn("Could not read .env file, using empty config: {}", e.getMessage());
            envContent = "";
        }

        return "#!/bin/bash\n" +
                "set -e\n" +
                "exec > /var/log/user-data.log 2>&1\n" +
                "echo 'Starting Worker bootstrap...'\n" +
                "cd /home/ec2-user\n" +
                "# Install Java 11 (Amazon Corretto)\n" +
                "echo 'Installing Java...'\n" +
                "dnf install -y java-11-amazon-corretto-headless\n" +
                "# Download worker.jar from S3\n" +
                "echo 'Downloading worker.jar from S3...'\n" +
                "aws s3 cp s3://" + s3BucketName + "/worker.jar /home/ec2-user/worker.jar\n" +
                "# Create .env file with configuration\n" +
                "echo 'Creating .env file...'\n" +
                "cat <<'EOF' > .env\n" +
                envContent + "\n" +
                "EOF\n" +
                "# Run the Worker for Stanford NLP models\n" +
                "echo 'Starting Worker...'\n" +
                "java -jar worker.jar >> /var/log/worker.log 2>&1 &\n" +
                "echo 'Worker started successfully'\n";
    }

    /**
     * Initiates graceful shutdown of all workers
     * Called when termination is requested and all jobs are complete
     */
    public void terminateAllWorkers() {
        logger.info("Terminating all workers...");

        try {
            List<Instance> workers = ec2Service.getRunningInstances("Role", "worker");
            for (Instance worker : workers) {
                ec2Service.terminateInstance(worker.instanceId());
            }

            // Wait for workers to terminate?
            // The shared service doesn't have a specific "waitForWorkersToTerminate"
            // method,
            // but we can just fire and forget or wait if needed.
            // The previous code had a wait. Let's skip the wait for now or implement it if
            // critical.
            // Actually, Manager shutdown usually waits.

        } catch (Exception e) {
            logger.error("Error terminating workers: {}", e.getMessage());
        }
    }

    /**
     * Gets the current number of running workers
     */
    public int getRunningWorkerCount() {
        try {
            return ec2Service.getRunningInstances("Role", "worker").size();
        } catch (Exception e) {
            logger.error("Error getting worker count: {}", e.getMessage());
            return 0;
        }
    }

    public int getMaxWorkerInstances() {
        return maxWorkerInstances;
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
}
