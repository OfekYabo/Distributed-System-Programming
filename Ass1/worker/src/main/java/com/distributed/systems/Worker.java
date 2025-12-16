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

    public Worker(AppConfig config) {
        this.running = new AtomicBoolean(true);

        // Load Configuration
        this.awsRegion = config.getString(AWS_REGION_KEY);
        this.s3BucketName = config.getString(S3_BUCKET_KEY);
        this.inputQueue = config.getString(INPUT_QUEUE_KEY);
        this.outputQueue = config.getString(OUTPUT_QUEUE_KEY);
        this.maxMessages = config.getIntOptional(MAX_MESSAGES_KEY, 1);
        this.waitTimeSeconds = config.getIntOptional(WAIT_TIME_KEY, 20);
        this.visibilityTimeout = config.getIntOptional(VISIBILITY_TIMEOUT_KEY, 90);
        this.idleShutdownSeconds = config.getIntOptional(IDLE_SHUTDOWN_KEY, 60); // Default 60s idle timeout

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

    private ExecutorService taskExecutor = Executors.newSingleThreadExecutor();

    /**
     * Processes a single message
     */
    private void processMessage(Message message) {
        logger.info("Processing message: {}", message.messageId());

        try {
            // Parse the message
            WorkerTaskMessage taskMessage = sqsService.parseMessage(message.body(), WorkerTaskMessage.class);
            logger.info("Parsed task message: {}", taskMessage);

            // Validate message type
            if (!WorkerTaskMessage.TYPE_URL_PARSE_REQUEST.equals(taskMessage.getType())) {
                logger.warn("Unknown message type: {}, ignoring", taskMessage.getType());
                sqsService.deleteMessage(inputQueue, message);
                return;
            }

            // Validate parsing method
            WorkerTaskMessage.TaskData taskData = taskMessage.getData();
            if (!TaskProcessor.isValidParsingMethod(taskData.getParsingMethod())) {
                String error = "Invalid parsing method: " + taskData.getParsingMethod();
                logger.error(error);
                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), error);
                sqsService.deleteMessage(inputQueue, message);
                return;
            }

            // Process the task with timeout
            long timeoutSeconds = Math.max(10, visibilityTimeout - 10);
            long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000);

            Future<String> future = null;
            try {
                future = taskExecutor.submit(() -> taskProcessor.processTask(taskData, deadline));

                // Wait for slightly less than visibility timeout to handle it before SQS does
                String s3Url = future.get(timeoutSeconds, TimeUnit.SECONDS);

                // Send success response
                sendSuccessResponse(taskData.getUrl(), s3Url, taskData.getParsingMethod());

                // Delete message from queue (only after successful processing)
                sqsService.deleteMessage(inputQueue, message);

            } catch (TimeoutException e) {
                if (future != null)
                    future.cancel(true); // Attempt to interrupt
                logger.error("Task timed out after {} seconds. Killing zombie thread.",
                        Math.max(10, visibilityTimeout - 10), e);

                // Nuclear Option: Kill the executor service to abandon the stuck thread
                taskExecutor.shutdownNow();
                taskExecutor = Executors.newSingleThreadExecutor();
                logger.warn("ExecutorService restarted to clear stuck thread.");

                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), "Task Timed Out");
                sqsService.deleteMessage(inputQueue, message); // Delete so we don't retry forever
            } catch (ExecutionException e) {
                logger.error("Failed to process task", e.getCause());
                String errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), errorMsg);
                sqsService.deleteMessage(inputQueue, message);
            } catch (InterruptedException e) {
                logger.error("Worker interrupted while waiting for task", e);
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            logger.error("Failed to parse or handle message", e);
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
     * Sends an error response message
     */
    private void sendErrorResponse(String fileUrl, String parsingMethod, String error) {
        WorkerTaskResult result = WorkerTaskResult.createError(fileUrl, parsingMethod, error);
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
            taskExecutor.shutdownNow();
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
