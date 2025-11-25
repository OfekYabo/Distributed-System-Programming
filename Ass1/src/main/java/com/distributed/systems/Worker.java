package com.distributed.systems;

import com.distributed.systems.model.WorkerErrorMessage;
import com.distributed.systems.model.WorkerResponseMessage;
import com.distributed.systems.model.WorkerTaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
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
    private final SqsMessageHandler sqsHandler;
    private final TaskProcessor taskProcessor;
    private final S3Uploader s3Uploader;
    private final AtomicBoolean running;
    
    public Worker(WorkerConfig config) {
        this.config = config;
        this.running = new AtomicBoolean(true);
        
        // Initialize AWS clients
        AwsClientFactory clientFactory = new AwsClientFactory(config);
        SqsClient sqsClient = clientFactory.createSqsClient();
        S3Client s3Client = clientFactory.createS3Client();
        
        // Initialize handlers
        this.sqsHandler = new SqsMessageHandler(sqsClient, config);
        this.s3Uploader = new S3Uploader(s3Client, config);
        this.taskProcessor = new TaskProcessor(config, s3Uploader);
        
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
                List<Message> messages = sqsHandler.receiveMessages();
                
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
            WorkerTaskMessage taskMessage = sqsHandler.parseTaskMessage(message.body());
            logger.info("Parsed task message: {}", taskMessage);
            
            // Validate message type
            if (!"urlParseRequest".equals(taskMessage.getType())) {
                logger.warn("Unknown message type: {}, ignoring", taskMessage.getType());
                sqsHandler.deleteMessage(message);
                return;
            }
            
            // Validate parsing method
            WorkerTaskMessage.TaskData taskData = taskMessage.getData();
            if (!TaskProcessor.isValidParsingMethod(taskData.getParsingMethod())) {
                String error = "Invalid parsing method: " + taskData.getParsingMethod();
                logger.error(error);
                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), error);
                sqsHandler.deleteMessage(message);
                return;
            }
            
            // Process the task
            try {
                String s3Url = taskProcessor.processTask(taskData);
                
                // Send success response
                sendSuccessResponse(taskData.getUrl(), s3Url, taskData.getParsingMethod());
                
                // Delete message from queue (only after successful processing)
                sqsHandler.deleteMessage(message);
                
            } catch (Exception e) {
                logger.error("Failed to process task", e);
                
                // Send error response
                String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                sendErrorResponse(taskData.getUrl(), taskData.getParsingMethod(), errorMsg);
                
                // Delete message from queue (we handled the error by sending error response)
                sqsHandler.deleteMessage(message);
            }
            
        } catch (Exception e) {
            logger.error("Failed to parse or handle message", e);
            // Don't delete the message - it will become visible again after visibility timeout
            // This allows for retry in case of transient errors
        }
    }
    
    /**
     * Sends a success response message
     */
    private void sendSuccessResponse(String fileUrl, String outputUrl, String parsingMethod) {
        WorkerResponseMessage.ResponseData responseData = 
                new WorkerResponseMessage.ResponseData(fileUrl, outputUrl, parsingMethod);
        
        WorkerResponseMessage responseMessage = 
                new WorkerResponseMessage("urlParseResponse", responseData);
        
        sqsHandler.sendResponseMessage(responseMessage);
    }
    
    /**
     * Sends an error response message
     */
    private void sendErrorResponse(String fileUrl, String parsingMethod, String error) {
        WorkerErrorMessage.ErrorData errorData = 
                new WorkerErrorMessage.ErrorData(fileUrl, parsingMethod, error);
        
        WorkerErrorMessage errorMessage = 
                new WorkerErrorMessage("urlParseError", errorData);
        
        sqsHandler.sendErrorMessage(errorMessage);
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
            sqsHandler.close();
            s3Uploader.close();
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

