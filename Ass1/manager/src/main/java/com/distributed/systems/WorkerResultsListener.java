package com.distributed.systems;

import com.distributed.systems.shared.AppConfig;
import com.distributed.systems.shared.model.LocalAppResponse;
import com.distributed.systems.shared.model.WorkerTaskResult;
import com.distributed.systems.shared.model.TaskResultMetadata;
import com.distributed.systems.shared.service.S3Service;
import com.distributed.systems.shared.service.SqsService;

import java.util.List;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread that listens for results from Workers
 * Processes success and error responses, triggers job completion
 */
public class WorkerResultsListener implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerResultsListener.class);

    // Config Keys
    private static final String WORKER_OUTPUT_QUEUE_KEY = "WORKER_OUTPUT_QUEUE";
    private static final String WAIT_TIME_KEY = "WAIT_TIME_SECONDS";
    private static final String VISIBILITY_TIMEOUT_KEY = "VISIBILITY_TIMEOUT_SECONDS";
    private static final String S3_PREFIX_KEY = "S3_MANAGER_OUTPUT_PREFIX";
    private static final String S3_BUCKET_KEY = "S3_BUCKET_NAME";
    private static final String LOCAL_APP_OUTPUT_QUEUE_KEY = "LOCAL_APP_OUTPUT_QUEUE";

    private final SqsService sqsService;
    private final S3Service s3Service;
    private final JobTracker jobTracker;
    private final AtomicBoolean running;

    // Config Values
    private final String workerOutputQueue;
    private final int waitTimeSeconds;
    private final int visibilityTimeout;
    private final String s3ManagerPrefix;
    private final String s3BucketName;
    private final String localAppOutputQueue;

    public WorkerResultsListener(AppConfig config,
            SqsService sqsService,
            S3Service s3Service,
            JobTracker jobTracker,
            AtomicBoolean running) {
        this.sqsService = sqsService;
        this.s3Service = s3Service;
        this.jobTracker = jobTracker;
        this.running = running;

        // Load Configuration
        this.workerOutputQueue = config.getString(WORKER_OUTPUT_QUEUE_KEY);
        this.waitTimeSeconds = config.getIntOptional(WAIT_TIME_KEY, 20);
        this.visibilityTimeout = config.getIntOptional(VISIBILITY_TIMEOUT_KEY, 180);
        this.s3ManagerPrefix = config.getOptional(S3_PREFIX_KEY, "manager-output");
        this.s3BucketName = config.getString(S3_BUCKET_KEY);
        this.localAppOutputQueue = config.getString(LOCAL_APP_OUTPUT_QUEUE_KEY);
    }

    @Override
    public void run() {
        logger.info("Started");

        while (running.get()) {
            try {
                // Poll for messages from workers
                List<Message> messages = sqsService.receiveMessages(workerOutputQueue, 10,
                        waitTimeSeconds, visibilityTimeout);

                for (Message message : messages) {
                    if (!running.get()) {
                        break;
                    }

                    processMessage(message);
                }

            } catch (Exception e) {
                logger.error("Error in WorkerResultsListener: {}", e.getMessage());
                sleep(5000);
            }
        }

        logger.info("Stopped");
    }

    /**
     * Processes a message from a worker
     */
    /**
     * Processes a message from a worker
     */
    private void processMessage(Message message) {
        try {
            // Parse the unified result message
            WorkerTaskResult result = sqsService.parseMessage(message.body(), WorkerTaskResult.class);
            WorkerTaskResult.ResultData data = result.getData();

            if (data == null) {
                logger.warn("Received empty result data");
                sqsService.deleteMessage(workerOutputQueue, message);
                return;
            }

            String jobId = data.getJobId();

            // Record task completion in JobTracker
            String completedJobId = jobTracker.recordTaskCompletion(jobId);

            if (completedJobId != null) {
                handleJobCompletion(completedJobId);
            }

            // Delete the message after successful processing
            sqsService.deleteMessage(workerOutputQueue, message);

        } catch (Exception e) {
            logger.error("Failed to process worker message: {}", e.getMessage());
            // Don't delete - will retry after visibility timeout
        }
    }

    /**
     * Handles the completion of a job
     */
    private void handleJobCompletion(String jobId) {
        logger.info("Handling job completion for Job ID: {}", jobId);

        JobTracker.JobInfo jobInfo = jobTracker.getJob(jobId);
        if (jobInfo == null) {
            logger.error("Job info not found for completed job ID: {}", jobId);
            return;
        }

        // Reconstruct reply queue URL: local-app-output-<jobId>
        String replyQueueUrl = "local-app-output-" + jobId;
        String inputFileS3Key = jobInfo.getInputFileS3Key();

        try {
            // 1. Aggregate results from S3 Metadata
            logger.info("Aggregating results from S3 for JobId: {}", jobId);
            String metadataPrefix = "results/" + jobId + "/metadata/";
            List<String> metadataKeys = s3Service.listObjects(metadataPrefix);

            List<TaskResultMetadata> aggregatedResults = new ArrayList<>();
            for (String key : metadataKeys) {
                try {
                    String json = s3Service.downloadAsString(key);
                    TaskResultMetadata meta = new com.fasterxml.jackson.databind.ObjectMapper()
                            .readValue(json, TaskResultMetadata.class);
                    aggregatedResults.add(meta);

                    // Optional: Delete metadata file after reading
                    s3Service.deleteFile(key);
                } catch (Exception e) {
                    logger.warn("Failed to process metadata for key {}: {}", key, e.getMessage());
                }
            }

            // 2. Generate Summary
            logger.info("Generating HTML summary...");
            String summaryHtml = HtmlSummaryGenerator.generateHtml(aggregatedResults);

            // 3. Upload Summary
            String summaryKey = "output/" + jobId + "_summary.html";
            s3Service.uploadString(summaryKey, summaryHtml, "text/html");

            // 4. Send Response
            logger.info("Sending response to local app at {}", replyQueueUrl);
            LocalAppResponse response = LocalAppResponse.taskComplete(inputFileS3Key, summaryKey);

            // Note: Manager needs permissions to send to this dynamically created queue
            // Ideally, LocalApp gave permissions or it's same account
            sqsService.sendMessage(replyQueueUrl, response);

            // 5. Cleanup Job
            jobTracker.removeJob(jobId);

        } catch (Exception e) {
            logger.error("Failed to handle job completion", e);
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
