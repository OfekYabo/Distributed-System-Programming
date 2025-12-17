package com.distributed.systems;

import com.distributed.systems.shared.AppConfig;
import com.distributed.systems.shared.model.WorkerTaskMessage;
import com.distributed.systems.shared.model.WorkerTaskResult;
import com.distributed.systems.shared.service.S3Service;
import com.distributed.systems.shared.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Worker main class
 * Continuously polls SQS for tasks, processes them, and sends results back
 */
public class Worker {

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    // Config keys
    private static final String INPUT_QUEUE_KEY = "WORKER_INPUT_QUEUE";
    private static final String OUTPUT_QUEUE_KEY = "WORKER_OUTPUT_QUEUE";
    private static final String S3_BUCKET_KEY = "S3_BUCKET_NAME";
    private static final String AWS_REGION_KEY = "AWS_REGION";
    private static final String WAIT_TIME_KEY = "WAIT_TIME_SECONDS";
    private static final String VISIBILITY_TIMEOUT_KEY = "VISIBILITY_TIMEOUT_SECONDS";
    private static final String MAX_MESSAGES_KEY = "WORKER_MAX_MESSAGES";
    private static final String IDLE_SHUTDOWN_KEY = "WORKER_IDLE_SHUTDOWN_SECONDS";
    private static final String HEARTBEAT_INTERVAL_KEY = "WORKER_HEARTBEAT_INTERVAL_SECONDS";

    private final SqsService sqsService;
    private final S3Service s3Service;
    private final TaskProcessor taskProcessor;
    private final AtomicBoolean running;

    // Config Values
    private final String awsRegion;
    private final String s3BucketName;
    private final String inputQueue;
    private final String outputQueue;
    private final int maxMessages;
    private final int waitTimeSeconds;
    private final int visibilityTimeout;
    private final int idleShutdownSeconds;
    private final int heartbeatIntervalSeconds;

    // Scheduled executor for heartbeats
    private final ScheduledExecutorService heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();

    public Worker(AppConfig config) {
        this.running = new AtomicBoolean(true);

        // Load Configuration
        this.awsRegion = config.getString(AWS_REGION_KEY);
        this.s3BucketName = config.getString(S3_BUCKET_KEY);
        this.inputQueue = config.getString(INPUT_QUEUE_KEY);
        this.outputQueue = config.getString(OUTPUT_QUEUE_KEY);
        this.maxMessages = config.getIntOptional(MAX_MESSAGES_KEY, 1);
        this.waitTimeSeconds = config.getIntOptional(WAIT_TIME_KEY, 20);
        this.visibilityTimeout = config.getIntOptional(VISIBILITY_TIMEOUT_KEY, 300); // Default 5 minutes
        this.idleShutdownSeconds = config.getIntOptional(IDLE_SHUTDOWN_KEY, 60); // Default 60s idle timeout
        // Heartbeat interval should be less than visibility timeout (e.g., half)
        this.heartbeatIntervalSeconds = config.getIntOptional(HEARTBEAT_INTERVAL_KEY, visibilityTimeout / 2);

        // Initialize AWS clients
        SqsClient sqsClient = SqsClient.builder().region(Region.of(awsRegion)).build();
        S3Client s3Client = S3Client.builder().region(Region.of(awsRegion)).build();

        this.sqsService = new SqsService(sqsClient);
        this.s3Service = new S3Service(s3Client, s3BucketName);

        // Initialize task processor
        this.taskProcessor = new TaskProcessor(config, s3Service);

        logger.info("Worker initialized. Idle shutdown set to {} seconds.", idleShutdownSeconds);
    }

    /**
     * Starts the worker's main processing loop
     */
    public void start() {
        logger.info("Worker starting...");

        // Setup shutdown hook for graceful termination
        setupShutdownHook();

        long lastActivityTime = System.currentTimeMillis();

        // Main processing loop
        while (running.get()) {
            try {
                // Poll for messages
                List<Message> messages = sqsService.receiveMessages(inputQueue, maxMessages, waitTimeSeconds,
                        visibilityTimeout);

                if (!messages.isEmpty()) {
                    lastActivityTime = System.currentTimeMillis();
                    // Process each message
                    for (Message message : messages) {
                        if (!running.get()) {
                            break;
                        }
                        processMessage(message);
                    }
                    // Update activity time again after processing
                    lastActivityTime = System.currentTimeMillis();
                } else {
                    // Check for idle timeout
                    if (System.currentTimeMillis() - lastActivityTime > idleShutdownSeconds * 1000L) {
                        logger.info("Worker idle for {} seconds. Shutting down.", idleShutdownSeconds);
                        running.set(false);
                    }
                }

            } catch (Exception e) {
                logger.error("Error in main loop", e);
                // Continue running even if there's an error
                sleep(5000);
            }
        }

        logger.info("Worker shutting down...");
        cleanup();
    }

    /**
     * Processes a single message with heartbeat mechanism and error-based re-queuing.
     * 
     * - Tasks can run as long as needed (no timeout)
     * - Heartbeat periodically extends visibility to prevent message becoming visible
     * - On error: re-queue with incremented retry count
     * - After MAX_RETRIES: send permanent error response
     */
    private void processMessage(Message message) {
        logger.info("Processing message: {}", message.messageId());

        WorkerTaskMessage.TaskData taskData = null;
        ScheduledFuture<?> heartbeatTask = null;
        final AtomicReference<String> receiptHandleRef = new AtomicReference<>(message.receiptHandle());

        try {
            // Parse the message
            WorkerTaskMessage taskMessage = sqsService.parseMessage(message.body(), WorkerTaskMessage.class);
            logger.info("Parsed task message: {}", taskMessage);

            // Validate message type
            if (!WorkerTaskMessage.TYPE_URL_PARSE_REQUEST.equals(taskMessage.getType())) {
                logger.warn("Unknown message type: {}, deleting", taskMessage.getType());
                sqsService.deleteMessage(inputQueue, message);
                return;
            }

            taskData = taskMessage.getData();

            // Validate parsing method (permanent error - don't retry)
            if (!TaskProcessor.isValidParsingMethod(taskData.getParsingMethod())) {
                String error = "Invalid parsing method: " + taskData.getParsingMethod();
                logger.error(error);
                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), error, taskData.getRetryCount());
                sqsService.deleteMessage(inputQueue, message);
                return;
            }

            logger.info("Starting task: url={}, method={}, retryCount={}", 
                    taskData.getUrl(), taskData.getParsingMethod(), taskData.getRetryCount());

            // Start heartbeat - periodically extend visibility timeout
            heartbeatTask = startHeartbeat(receiptHandleRef);

            // Process the task (NO TIMEOUT - can run as long as needed)
            String s3Url = taskProcessor.processTask(taskData, Long.MAX_VALUE);

            // Success! Stop heartbeat, send response, delete message
            stopHeartbeat(heartbeatTask);
            sendSuccessResponse(taskData.getUrl(), s3Url, taskData.getParsingMethod());
            sqsService.deleteMessage(inputQueue, message);
            logger.info("Task completed successfully: {}", taskData.getUrl());

        } catch (Exception e) {
            // Stop heartbeat first
            stopHeartbeat(heartbeatTask);

            logger.error("Task failed with error: {}", e.getMessage(), e);

            if (taskData != null) {
                handleTaskError(message, taskData, e);
            } else {
                // Couldn't even parse the message - delete it to prevent infinite loop
                logger.error("Could not parse message, deleting to prevent infinite retry");
                sqsService.deleteMessage(inputQueue, message);
            }
        }
    }

    /**
     * Handles task errors by either re-queuing or marking as permanent failure
     */
    private void handleTaskError(Message originalMessage, WorkerTaskMessage.TaskData taskData, Exception error) {
        String errorMsg = error.getMessage() != null ? error.getMessage() : error.getClass().getSimpleName();

        if (taskData.hasExceededMaxRetries()) {
            // Max retries exceeded - permanent failure
            logger.error("Task permanently failed after {} retries: {} / {}", 
                    WorkerTaskMessage.TaskData.MAX_RETRIES, taskData.getUrl(), taskData.getParsingMethod());
            sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), errorMsg, taskData.getRetryCount());
            sqsService.deleteMessage(inputQueue, originalMessage);
        } else {
            // Re-queue with incremented retry count
            int nextRetry = taskData.getRetryCount() + 1;
            logger.warn("Re-queuing task for retry {}/{}: {} / {} - Error: {}", 
                    nextRetry, WorkerTaskMessage.TaskData.MAX_RETRIES,
                    taskData.getUrl(), taskData.getParsingMethod(), errorMsg);

            WorkerTaskMessage retryMessage = WorkerTaskMessage.createWithRetry(
                    taskData.getUrl(), taskData.getParsingMethod(), nextRetry);
            sqsService.sendMessage(inputQueue, retryMessage);
            
            // Delete original message (we've re-queued a new one)
            sqsService.deleteMessage(inputQueue, originalMessage);
        }
    }

    /**
     * Starts the heartbeat scheduler to periodically extend message visibility
     */
    private ScheduledFuture<?> startHeartbeat(AtomicReference<String> receiptHandleRef) {
        logger.debug("Starting heartbeat with interval {} seconds", heartbeatIntervalSeconds);
        
        return heartbeatScheduler.scheduleAtFixedRate(() -> {
            try {
                String receiptHandle = receiptHandleRef.get();
                sqsService.changeMessageVisibility(inputQueue, receiptHandle, visibilityTimeout);
                logger.debug("Heartbeat: extended visibility timeout to {} seconds", visibilityTimeout);
            } catch (Exception e) {
                logger.warn("Heartbeat failed to extend visibility: {}", e.getMessage());
            }
        }, heartbeatIntervalSeconds, heartbeatIntervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Stops the heartbeat task
     */
    private void stopHeartbeat(ScheduledFuture<?> heartbeatTask) {
        if (heartbeatTask != null && !heartbeatTask.isCancelled()) {
            heartbeatTask.cancel(false);
            logger.debug("Heartbeat stopped");
        }
    }

    /**
     * Sends a success response message
     */
    private void sendSuccessResponse(String fileUrl, String outputUrl, String parsingMethod) {
        WorkerTaskResult result = WorkerTaskResult.createSuccess(fileUrl, outputUrl, parsingMethod);
        sqsService.sendMessage(outputQueue, result);
    }

    /**
     * Sends an error response message (only after max retries exceeded)
     */
    private void sendErrorResponse(String fileUrl, String parsingMethod, String error, int retryCount) {
        String fullError = String.format("%s (after %d retries)", error, retryCount);
        WorkerTaskResult result = WorkerTaskResult.createError(fileUrl, parsingMethod, fullError);
        sqsService.sendMessage(outputQueue, result);
    }

    /**
     * Sets up shutdown hook for graceful termination
     */
    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            running.set(false);
        }));
    }

    /**
     * Sleeps for specified milliseconds
     */
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Cleanup resources
     */
    private void cleanup() {
        try {
            heartbeatScheduler.shutdownNow();
            sqsService.close();
            s3Service.close();
            logger.info("Cleanup completed");
        } catch (Exception e) {
            logger.error("Error during cleanup", e);
        }
    }

    /**
     * Main entry point
     */
    public static void main(String[] args) {
        logger.info("=== Worker Application Starting ===");

        try {
            AppConfig config = new AppConfig();
            Worker worker = new Worker(config);
            worker.start();
        } catch (Exception e) {
            logger.error("Fatal error in worker", e);
            System.exit(1);
        }

        logger.info("=== Worker Application Stopped ===");
    }
}
