package com.distributed.systems;

import com.distributed.systems.shared.AppConfig;

import com.distributed.systems.shared.service.Ec2Service;
import com.distributed.systems.shared.service.S3Service;
import com.distributed.systems.shared.service.SqsService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sqs.SqsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manager main class
 * Orchestrates all components: LocalAppListener, WorkerResultsListener,
 * WorkerScaler
 * Handles lifecycle management and graceful shutdown
 */
public class Manager {

    private static final Logger logger = LoggerFactory.getLogger(Manager.class);

    // Config Keys
    private static final String AWS_REGION_KEY = "AWS_REGION";
    private static final String S3_BUCKET_KEY = "S3_BUCKET_NAME";
    private static final String LOCAL_APP_INPUT_QUEUE_KEY = "LOCAL_APP_INPUT_QUEUE";
    private static final String WORKER_INPUT_QUEUE_KEY = "WORKER_INPUT_QUEUE";
    private static final String WORKER_OUTPUT_QUEUE_KEY = "WORKER_OUTPUT_QUEUE";
    private static final String WORKER_CONTROL_QUEUE_KEY = "WORKER_CONTROL_QUEUE";
    private static final String VISIBILITY_TIMEOUT_KEY = "VISIBILITY_TIMEOUT_SECONDS";
    private static final String WAIT_TIME_KEY = "WAIT_TIME_SECONDS";
    private static final String IDLE_TIMEOUT_KEY = "MANAGER_IDLE_TIMEOUT_MINUTES";

    // Config Values (Manager's own)
    // Wait, if I pass config to listeners, I still need it or the listeners should
    // take individual values?
    // Listeners take AppConfig and extract their own values (as I just
    // implemented).
    // So Manager still needs AppConfig to pass to listeners.
    // But Manager's OWN configuration should be field-based.

    // Config Values (Manager's own)
    private final String awsRegion;
    private final String s3BucketName;
    private final String localAppInputQueue;
    // private final String localAppOutputQueue; // Removed as unused
    private final String workerInputQueue;
    private final String workerOutputQueue;
    private final String workerControlQueue;
    private final int visibilityTimeout;
    private final int waitTimeSeconds;
    private final int idleTimeoutMinutes;

    private final SqsService sqsService;
    private final S3Service s3Service;
    private final Ec2Service ec2Service;
    private final JobTracker jobTracker;

    private final AtomicBoolean running;
    private final AtomicBoolean acceptingJobs;
    private final AtomicBoolean terminateRequested;

    private ExecutorService executorService;
    private LocalAppListener localAppListener;
    private WorkerResultsListener workerResultsListener;
    private WorkerScaler workerScaler;

    private long lastActivityTime;

    public Manager(AppConfig config) {
        this.running = new AtomicBoolean(true);
        this.acceptingJobs = new AtomicBoolean(true);
        this.terminateRequested = new AtomicBoolean(false);

        // Load Configuration (Manager's)
        this.awsRegion = config.getString(AWS_REGION_KEY);
        this.s3BucketName = config.getString(S3_BUCKET_KEY);
        this.localAppInputQueue = config.getString(LOCAL_APP_INPUT_QUEUE_KEY);
        // this.localAppOutputQueue = config.getString(LOCAL_APP_OUTPUT_QUEUE_KEY);
        this.workerInputQueue = config.getString(WORKER_INPUT_QUEUE_KEY);
        this.workerOutputQueue = config.getString(WORKER_OUTPUT_QUEUE_KEY);
        this.workerControlQueue = config.getOptional(WORKER_CONTROL_QUEUE_KEY, "WorkerControlQueue");
        this.visibilityTimeout = config.getIntOptional(VISIBILITY_TIMEOUT_KEY, 180);
        this.waitTimeSeconds = config.getIntOptional(WAIT_TIME_KEY, 20);
        this.idleTimeoutMinutes = config.getIntOptional(IDLE_TIMEOUT_KEY, 30);

        // Initialize services
        SqsClient sqsClient = SqsClient.builder().region(Region.of(awsRegion)).build();
        this.sqsService = new SqsService(sqsClient);

        S3Presigner s3Presigner = S3Presigner.builder().region(Region.of(awsRegion)).build();
        S3Client s3Client = S3Client.builder().region(Region.of(awsRegion)).build();
        this.s3Service = new S3Service(s3Client, s3Presigner, s3BucketName);

        Ec2Client ec2Client = Ec2Client.builder().region(Region.of(awsRegion)).build();
        this.ec2Service = new Ec2Service(ec2Client);

        // Initialize job tracker
        this.jobTracker = new JobTracker();

        // Initialize threads
        this.localAppListener = new LocalAppListener(
                config, sqsService, s3Service, jobTracker, running, acceptingJobs, terminateRequested);

        // HtmlSummaryGenerator is static now
        this.workerResultsListener = new WorkerResultsListener(config, sqsService, s3Service,
                jobTracker, running);

        this.workerScaler = new WorkerScaler(
                config, sqsService, ec2Service, jobTracker, running, terminateRequested);

        this.lastActivityTime = System.currentTimeMillis();

        logger.info("Initialized (bucket: {}, region: {})", s3BucketName, awsRegion);
    }

    /**
     * Starts the manager
     */
    public void start() {
        logger.info("=== Manager Starting ===");
        System.out.println("DEBUG: Manager application starting (v2 - with env export fix)...");

        // Ensure all SQS queues exist
        ensureQueuesExist();

        // Setup shutdown hook
        setupShutdownHook();

        // Create thread pool for the three main threads
        executorService = Executors.newFixedThreadPool(3);

        // Start all threads
        executorService.submit(localAppListener);
        executorService.submit(workerResultsListener);
        executorService.submit(workerScaler);

        logger.info("All threads started");

        // Main monitoring loop
        monitorAndManage();

        // Cleanup
        shutdown();
    }

    private void ensureQueuesExist() {
        // Ensure queues exist
        sqsService.createQueueIfNotExists(localAppInputQueue, visibilityTimeout, waitTimeSeconds);
        // Removed LOCAL_APP_OUTPUT_QUEUE as per user request (redundant)
        // sqsService.createQueueIfNotExists(localAppOutputQueue, visibilityTimeout,
        // waitTimeSeconds);
        sqsService.createQueueIfNotExists(workerInputQueue, visibilityTimeout, waitTimeSeconds);
        sqsService.createQueueIfNotExists(workerOutputQueue, visibilityTimeout, waitTimeSeconds);
        sqsService.createQueueIfNotExists(workerControlQueue, visibilityTimeout, waitTimeSeconds);
    }

    /**
     * Main monitoring loop
     * Checks for termination conditions and idle timeout
     */
    private void monitorAndManage() {
        while (running.get()) {
            try {
                // Check if we should terminate
                if (shouldTerminate()) {
                    logger.info("Terminating...");
                    running.set(false);
                    break;
                }

                // Update activity time if there are active jobs
                if (jobTracker.hasActiveJobs()) {
                    lastActivityTime = System.currentTimeMillis();
                }

                // Log status periodically
                logStatus();

                // Sleep before next check
                Thread.sleep(10000); // Check every 10 seconds

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in monitor loop: {}", e.getMessage());
            }
        }
    }

    /**
     * Checks if manager should terminate
     */
    private boolean shouldTerminate() {
        // If terminate was requested
        if (terminateRequested.get()) {
            // Wait for all jobs to complete
            if (!jobTracker.hasActiveJobs()) {
                logger.info("Terminate requested and all jobs complete");
                return true;
            }
            logger.debug("Terminate requested but {} jobs still active", jobTracker.getActiveJobCount());
            return false;
        }

        // Check idle timeout
        long idleTimeMinutes = (System.currentTimeMillis() - lastActivityTime) / (60 * 1000);
        if (idleTimeMinutes >= idleTimeoutMinutes) {
            logger.info("Idle timeout reached ({} minutes)", idleTimeMinutes);
            return true;
        }

        return false;
    }

    /**
     * Logs current status
     */
    private void logStatus() {
        int activeJobs = jobTracker.getActiveJobCount();
        int pendingTasks = jobTracker.getTotalPendingTasks();
        int runningWorkers = workerScaler.getRunningWorkerCount();
        int wantedWorkers = jobTracker.getGlobalNeededWorkers();
        int maxWorkers = workerScaler.getMaxWorkerInstances();

        logger.info("jobs={} pending={} workers={} (wanted={}, max={})",
                activeJobs, pendingTasks, runningWorkers, wantedWorkers, maxWorkers);
    }

    /**
     * Graceful shutdown
     */
    private void shutdown() {
        logger.info("=== Manager Shutting Down ===");

        // Stop accepting new jobs
        acceptingJobs.set(false);

        // Signal threads to stop
        running.set(false);

        // Shutdown executor service
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }

        // Terminate all workers
        logger.info("Terminating all workers...");
        workerScaler.terminateAllWorkers();

        // Close services
        closeServices();

        logger.info("=== Manager Stopped ===");

        // Self-Terminate EC2 Instance
        terminateSelf();
    }

    private void terminateSelf() {
        try {
            String instanceId = retrieveInstanceId();
            if (instanceId != null) {
                logger.warn(">>> SELF-TERMINATING MANAGER INSTANCE: {} <<<", instanceId);
                // Re-create EC2 client if services are closed?
                // We closed services above. We should probably keep Ec2Service open or re-open.
                // Or just move closeServices() after terminateSelf?
                // But terminateSelf needs Ec2Service.
                // Let's modify logic: call terminateSelf BEFORE closeServices,
                // OR re-instantiate client.

                // Since we closed services, let's just make a fresh client for the final kill
                // command
                // to be safe and independent of shared service state.
                try (Ec2Client killerClient = Ec2Client.builder().region(Region.of(awsRegion)).build();
                        Ec2Service killerService = new Ec2Service(killerClient)) {
                    killerService.terminateInstance(instanceId);
                }
            } else {
                logger.error("Could not retrieve instance ID. Exiting process only.");
            }
        } catch (Exception e) {
            logger.error("Error during self-termination", e);
        }
    }

    private String retrieveInstanceId() {
        try {
            java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                    .connectTimeout(java.time.Duration.ofSeconds(2))
                    .build();

            // Try IMDSv2 (Token-based)
            String token = null;
            try {
                java.net.http.HttpRequest tokenRequest = java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create("http://169.254.169.254/latest/api/token"))
                        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                        .PUT(java.net.http.HttpRequest.BodyPublishers.noBody())
                        .build();
                java.net.http.HttpResponse<String> tokenResponse = client.send(tokenRequest,
                        java.net.http.HttpResponse.BodyHandlers.ofString());
                if (tokenResponse.statusCode() == 200) {
                    token = tokenResponse.body();
                }
            } catch (Exception ignored) {
                // Fallback to IMDSv1
            }

            // Get Instance ID (with token if available)
            java.net.http.HttpRequest.Builder requestBuilder = java.net.http.HttpRequest.newBuilder()
                    .uri(java.net.URI.create("http://169.254.169.254/latest/meta-data/instance-id"))
                    .GET();

            if (token != null) {
                requestBuilder.header("X-aws-ec2-metadata-token", token);
            }

            java.net.http.HttpResponse<String> response = client.send(requestBuilder.build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return response.body();
            }
        } catch (Exception e) {
            logger.warn("Failed to retrieve instance ID via IMDS: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Closes all services
     */
    private void closeServices() {
        try {
            sqsService.close();
        } catch (Exception e) {
            logger.error("Error closing SqsService: {}", e.getMessage());
        }

        try {
            s3Service.close();
        } catch (Exception e) {
            logger.error("Error closing S3Service: {}", e.getMessage());
        }

        try {
            ec2Service.close();
        } catch (Exception e) {
            logger.error("Error closing Ec2Service: {}", e.getMessage());
        }
    }

    /**
     * Sets up shutdown hook for graceful termination on SIGTERM/SIGINT
     */
    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            running.set(false);
            // On SIGTERM (from AWS termination), we don't need to self-terminate again
            // but for "nuclear option" consistency, it probably won't hurt if we try.
        }));
    }

    /**
     * Main entry point
     */
    public static void main(String[] args) {
        // 1. Immediate Printf Debugging - Visible in /var/log/manager.log
        System.out.println("=================================================");
        System.out.println("DEBUG: Manager Process Starting (Robust Loader)");
        System.out.println("DEBUG: Current Time: " + System.currentTimeMillis());

        try {
            // 2. Load .env file explicitly into System Properties
            // This ensures Logback (AWS_REGION) and AWS SDK (Credentials) see them
            // even if they weren't exported to Shell Env
            java.io.File envFile = new java.io.File(".env");
            if (envFile.exists()) {
                System.out.println("DEBUG: Found .env file, loading variables...");
                java.util.List<String> lines = java.nio.file.Files.readAllLines(envFile.toPath());
                for (String line : lines) {
                    if (line.trim().isEmpty() || line.startsWith("#"))
                        continue;
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        // Set as System Property (overrides env vars for Java libs)
                        System.setProperty(key, value);
                        // Also helpful for debug (mask secrets)
                        if (!key.contains("SECRET")) {
                            System.out.println("DEBUG: Set Property: " + key + "=" + value);
                        }
                    }
                }
            } else {
                System.out.println("DEBUG: WARNING - No .env file found!");
            }
        } catch (Exception e) {
            System.err.println("DEBUG: Failed to load .env file: " + e.getMessage());
            e.printStackTrace();
        }

        logger.info("=== Manager Application Starting ===");

        try {
            AppConfig config = new AppConfig(); // Now sees System Properties
            Manager manager = new Manager(config);
            manager.start();
        } catch (Exception e) {
            logger.error("Fatal error in manager: {}", e.getMessage());
            System.err.println("FATAL MANAGER ERROR: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        logger.info("=== Manager Application Exited ===");
    }
}
