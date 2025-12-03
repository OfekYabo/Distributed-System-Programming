package com.distributed.systems;

import com.distributed.systems.shared.AwsClientFactory;
import com.distributed.systems.shared.model.WorkerTaskMessage;
import com.distributed.systems.shared.model.WorkerTaskResult;
import com.distributed.systems.shared.service.S3Service;
import com.distributed.systems.shared.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Worker main class
 * Continuously polls SQS for tasks, processes them, and sends results back
 */
public class Worker {

    private static final Logger logger = LoggerFactory.getLogger(Worker.class);

    private final WorkerConfig config;
    private final SqsService sqsService;
    private final S3Service s3Service;
    private final TaskProcessor taskProcessor;
    private final AtomicBoolean running;

    public Worker(WorkerConfig config) {
        this.config = config;
        this.running = new AtomicBoolean(true);

        // Initialize AWS clients using factory
        String region = config.getAwsRegion();
        this.sqsService = new SqsService(AwsClientFactory.createSqsClient(region));
        this.s3Service = new S3Service(AwsClientFactory.createS3Client(region), config.getS3BucketName());

        // Initialize task processor with shared S3 service
        this.taskProcessor = new TaskProcessor(config, s3Service);

        logger.info("Worker initialized with config: {}", config);
    }

    /**
     * Starts the worker's main processing loop
     */
    public void start() {
        logger.info("Worker starting...");

        // Setup shutdown hook for graceful termination
        setupShutdownHook();

        // Main processing loop
        while (running.get()) {
            try {
                // Poll for messages
                List<Message> messages = sqsService.receiveMessages(config.getInputQueueName(), 1, 20, 20);

                // Process each message
                for (Message message : messages) {
                    if (!running.get()) {
                        break;
                    }

                    processMessage(message);
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
                sqsService.deleteMessage(config.getInputQueueName(), message);
                return;
            }

            // Validate parsing method
            WorkerTaskMessage.TaskData taskData = taskMessage.getData();
            if (!TaskProcessor.isValidParsingMethod(taskData.getParsingMethod())) {
                String error = "Invalid parsing method: " + taskData.getParsingMethod();
                logger.error(error);
                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), error);
                sqsService.deleteMessage(config.getInputQueueName(), message);
                return;
            }

            // Process the task
            try {
                String s3Url = taskProcessor.processTask(taskData);

                // Send success response
                sendSuccessResponse(taskData.getUrl(), s3Url, taskData.getParsingMethod());

                // Delete message from queue (only after successful processing)
                sqsService.deleteMessage(config.getInputQueueName(), message);

            } catch (Exception e) {
                logger.error("Failed to process task", e);

                // Send error response
                String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), errorMsg);

                // Delete message from queue (we handled the error by sending error response)
                sqsService.deleteMessage(config.getInputQueueName(), message);
            }

        } catch (Exception e) {
            logger.error("Failed to parse or handle message", e);
            // Don't delete the message - it will become visible again after visibility
            // timeout
            // This allows for retry in case of transient errors
        }
    }

    /**
     * Sends a success response message
     */
    private void sendSuccessResponse(String fileUrl, String outputUrl, String parsingMethod) {
        WorkerTaskResult result = WorkerTaskResult.createSuccess(fileUrl, outputUrl, parsingMethod);
        sqsService.sendMessage(config.getOutputQueueName(), result);
    }

    /**
     * Sends an error response message
     */
    private void sendErrorResponse(String fileUrl, String parsingMethod, String error) {
        WorkerTaskResult result = WorkerTaskResult.createError(fileUrl, parsingMethod, error);
        sqsService.sendMessage(config.getOutputQueueName(), result);
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
            WorkerConfig config = new WorkerConfig();
            Worker worker = new Worker(config);
            worker.start();
        } catch (Exception e) {
            logger.error("Fatal error in worker", e);
            System.exit(1);
        }

        logger.info("=== Worker Application Stopped ===");
    }
}
