package com.distributed.systems;

import com.distributed.systems.shared.AppConfig;
import com.distributed.systems.shared.model.LocalAppRequest;
import com.distributed.systems.shared.model.WorkerTaskMessage;
import com.distributed.systems.shared.service.S3Service;
import com.distributed.systems.shared.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread that listens for messages from Local Applications
 * Handles new task requests and termination requests
 */
public class LocalAppListener implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LocalAppListener.class);

    // Config Keys
    private static final String INPUT_QUEUE_KEY = "LOCAL_APP_INPUT_QUEUE";
    private static final String WORKER_QUEUE_KEY = "WORKER_INPUT_QUEUE";
    private static final String WAIT_TIME_KEY = "WAIT_TIME_SECONDS";
    private static final String VISIBILITY_TIMEOUT_KEY = "VISIBILITY_TIMEOUT_SECONDS";

    private final SqsService sqsService;
    private final S3Service s3Service;
    private final JobTracker jobTracker;
    private final AtomicBoolean running;
    private final AtomicBoolean acceptingJobs;
    private final AtomicBoolean terminateRequested;

    // Config Values (Eagerly Loaded)
    private final String inputQueueUrl;
    private final String workerQueueUrl;
    private final int waitTimeSeconds;
    private final int visibilityTimeout;

    public LocalAppListener(AppConfig config,
            SqsService sqsService,
            S3Service s3Service,
            JobTracker jobTracker,
            AtomicBoolean running,
            AtomicBoolean acceptingJobs,
            AtomicBoolean terminateRequested) {
        // Services
        this.sqsService = sqsService;
        this.s3Service = s3Service;
        this.jobTracker = jobTracker;
        this.running = running;
        this.acceptingJobs = acceptingJobs;
        this.terminateRequested = terminateRequested;

        // Load Configuration (Fail Fast)
        this.inputQueueUrl = config.getString(INPUT_QUEUE_KEY);
        this.workerQueueUrl = config.getString(WORKER_QUEUE_KEY);
        this.waitTimeSeconds = config.getIntOptional(WAIT_TIME_KEY, 20);
        this.visibilityTimeout = config.getIntOptional(VISIBILITY_TIMEOUT_KEY, 180);
    }

    @Override
    public void run() {
        logger.info("Started");

        while (running.get()) {
            try {
                // Poll for messages from local applications
                List<Message> messages = sqsService.receiveMessages(inputQueueUrl, 1,
                        waitTimeSeconds, visibilityTimeout);

                for (Message message : messages) {
                    if (!running.get()) {
                        break;
                    }

                    processMessage(message);
                }

            } catch (Exception e) {
                logger.error("Error in LocalAppListener: {}", e.getMessage());
                sleep(5000);
            }
        }

        logger.info("Stopped");
    }

    /**
     * Processes a message from a local application
     */
    private void processMessage(Message message) {
        try {
            LocalAppRequest request = sqsService.parseMessage(message.body(), LocalAppRequest.class);
            logger.info("Received message from local app: {}", request);

            if (request.isTerminate()) {
                handleTerminate(message);
            } else if (request.isNewTask()) {
                handleNewTask(message, request);
            } else {
                logger.warn("Unknown message type: {}", request.getType());
                sqsService.deleteMessage(inputQueueUrl, message);
            }

        } catch (Exception e) {
            logger.error("Failed to process message from local app: {}", e.getMessage());
            // Don't delete - will retry after visibility timeout
        }
    }

    /**
     * Handles a termination request
     */
    private void handleTerminate(Message message) {
        logger.info("Received termination request");

        // Stop accepting new jobs
        acceptingJobs.set(false);
        terminateRequested.set(true);

        // Delete the message
        sqsService.deleteMessage(inputQueueUrl, message);

        logger.info("Termination request processed - no longer accepting new jobs");
    }

    /**
     * Handles a new task request
     */
    private void handleNewTask(Message message, LocalAppRequest request) {
        if (!acceptingJobs.get()) {
            logger.warn("Rejecting new task - manager is shutting down");
            // Don't delete - let it become visible again for another manager
            return;
        }

        LocalAppRequest.RequestData data = request.getData();
        if (data == null || data.getInputFileS3Key() == null) {
            logger.error("Invalid task request - missing data");
            sqsService.deleteMessage(inputQueueUrl, message);
            return;
        }

        String inputFileS3Key = data.getInputFileS3Key();
        int n = data.getN() > 0 ? data.getN() : 1;
        String jobId = request.getData().getJobId();
        // Derived from JobID
        String replyQueueUrl = "local-app-output-" + jobId;
        logger.info("New task received. Input: {}, N: {}, ReplyQueue: {}, JobID: {}",
                inputFileS3Key, n, replyQueueUrl, jobId);

        try {
            // Download the input file from S3
            String content = s3Service.downloadAsString(inputFileS3Key);
            List<String> lines = Arrays.asList(content.split("\\r?\\n"));
            logger.info("Downloaded input file with {} lines", lines.size());

            // Parse the input file and create tasks
            // Parse the input file and create tasks
            List<WorkerTaskMessage> taskMessages = new ArrayList<>();
            String currentJobId = data.getJobId();

            for (String line : lines) {
                String[] parts = line.split("\t");
                if (parts.length >= 2) {
                    String parsingMethod = parts[0].trim();
                    String url = parts[1].trim();

                    if (!parsingMethod.isEmpty() && !url.isEmpty()) {
                        taskMessages.add(WorkerTaskMessage.createTask(parsingMethod, url, currentJobId));
                    }
                }
            }

            if (taskMessages.isEmpty()) {
                logger.warn("No valid tasks found in input file: {}", inputFileS3Key);
                sqsService.deleteMessage(inputQueueUrl, message);
                return;
            }

            int totalTasks = taskMessages.size();
            // Register the job with the JobTracker
            // Queue URL is derived from Job ID
            jobTracker.registerJob(jobId, inputFileS3Key, totalTasks, n);

            // Send tasks to worker queue
            for (WorkerTaskMessage taskMsg : taskMessages) {
                sqsService.sendMessage(workerQueueUrl, taskMsg);
            }

            logger.info("Sent {} tasks to worker queue for job {}", taskMessages.size(), inputFileS3Key);

            // Delete the original message
            sqsService.deleteMessage(inputQueueUrl, message);

            // Cleanup: Delete the input file from S3
            logger.info("Cleaning up: Deleting input file {} from S3...", inputFileS3Key);
            try {
                s3Service.deleteFile(inputFileS3Key);
                logger.info("Input file deleted: {}", inputFileS3Key);
            } catch (Exception e) {
                logger.warn("Failed to delete input file {}: {}", inputFileS3Key, e.getMessage());
            }

        } catch (Exception e) {
            logger.error("Failed to process new task {}: {}", inputFileS3Key, e.getMessage());
            // Don't delete - will retry after visibility timeout
        }
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
