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
    private final HtmlSummaryGenerator htmlGenerator;
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
            HtmlSummaryGenerator htmlGenerator,
            AtomicBoolean running) {
        this.sqsService = sqsService;
        this.s3Service = s3Service;
        this.jobTracker = jobTracker;
        this.htmlGenerator = htmlGenerator;
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
    private void processMessage(Message message) {
        try {
            // Parse the unified result message
            WorkerTaskResult result = sqsService.parseMessage(message.body(), WorkerTaskResult.class);
            WorkerTaskResult.ResultData data = result.getData();

            String completedJobKey;

            // Legacy logging/tracking removed in favor of handleResult
            if (data.isSuccess()) {
                logger.info("Worker success: {} / {} -> {}",
                        data.getFileUrl(), data.getParsingMethod(), data.getOutputUrl());
            } else {
                logger.warn("Worker error: {} / {} - {}",
                        data.getFileUrl(), data.getParsingMethod(), data.getErrorMessage());
            }

            handleResult(result);

            // Delete the message after successful processing
            sqsService.deleteMessage(workerOutputQueue, message);

        } catch (Exception e) {
            logger.error("Failed to process worker message: {}", e.getMessage());
            // Don't delete - will retry after visibility timeout
        }
    }

    private void handleResult(WorkerTaskResult result) {
        WorkerTaskResult.ResultData data = result.getData();
        if (data == null) {
            logger.warn("Received empty result data");
            return;
        }

        String jobId = data.getJobId();
        // Since we are stateless, we need a way to find the inputFileS3Key from the
        // JobId?
        // JobInfo now has JobId. We can search active jobs by JobId.
        String inputFileS3Key = jobTracker.findInputFileKeyByJobId(jobId);

        if (inputFileS3Key == null) {
            logger.warn("Received result for unknown Job ID: {}", jobId);
            return;
        }

        String completedReq = null;

        if (data.isSuccess()) {
            completedReq = jobTracker.recordSuccess(inputFileS3Key);
        } else {
            completedReq = jobTracker.recordError(inputFileS3Key);
        }

        if (completedReq != null) {
            handleJobCompletion(completedReq);
        }
    }

    /**
     * Handles the completion of a job
     */
    private void handleJobCompletion(String inputFileS3Key) {
        logger.info("Handling job completion: {}", inputFileS3Key);

        JobTracker.JobInfo jobInfo = jobTracker.getJob(inputFileS3Key);
        if (jobInfo == null) {
            logger.error("Job info not found for completed job: {}", inputFileS3Key);
            return;
        }

        String replyQueueUrl = jobInfo.getReplyQueueUrl();
        String jobId = jobInfo.getJobId();

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
            // Also clean up the metadata folder if empty? (S3 folders are virtual)

            // 2. Generate Summary
            logger.info("Generating HTML summary...");
            String summaryHtml = HtmlSummaryGenerator.generateHtml(aggregatedResults);

            // 3. Upload Summary
            String summaryKey = "output/" + jobId + "_summary.html";
            s3Service.uploadString(summaryKey, summaryHtml, "text/html");

            // 4. Send Response
            logger.info("Sending response to local app...");
            LocalAppResponse response = LocalAppResponse.taskComplete(inputFileS3Key, summaryKey);
            sqsService.sendMessage(replyQueueUrl, response);

            // 5. Cleanup Job
            jobTracker.removeJob(inputFileS3Key);

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
