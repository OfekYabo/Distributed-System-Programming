package com.distributed.systems;

import com.distributed.systems.model.LocalAppResponse;
import com.distributed.systems.model.WorkerErrorResponse;
import com.distributed.systems.model.WorkerResponse;
import com.distributed.systems.model.WorkerSuccessResponse;
import com.distributed.systems.service.S3Service;
import com.distributed.systems.service.SqsService;
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
    
    private final ManagerConfig config;
    private final SqsService sqsService;
    private final S3Service s3Service;
    private final JobTracker jobTracker;
    private final HtmlSummaryGenerator htmlGenerator;
    private final AtomicBoolean running;
    
    public WorkerResultsListener(ManagerConfig config,
                                SqsService sqsService,
                                S3Service s3Service,
                                JobTracker jobTracker,
                                HtmlSummaryGenerator htmlGenerator,
                                AtomicBoolean running) {
        this.config = config;
        this.sqsService = sqsService;
        this.s3Service = s3Service;
        this.jobTracker = jobTracker;
        this.htmlGenerator = htmlGenerator;
        this.running = running;
    }
    
    @Override
    public void run() {
        logger.info("Started");
        
        while (running.get()) {
            try {
                // Poll for messages from workers
                List<Message> messages = sqsService.receiveMessages(config.getWorkerOutputQueue());
                
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
            // First, determine the message type
            WorkerResponse baseResponse = sqsService.parseMessage(message.body(), WorkerResponse.class);
            
            String completedJobKey;
            
            if (baseResponse.isSuccess()) {
                completedJobKey = handleSuccessResponse(message);
            } else if (baseResponse.isError()) {
                completedJobKey = handleErrorResponse(message);
            } else {
                logger.warn("Unknown worker response type: {}", baseResponse.getType());
                sqsService.deleteMessage(config.getWorkerOutputQueue(), message);
                return;
            }
            
            // If a job was completed, generate and upload summary
            if (completedJobKey != null) {
                handleJobCompletion(completedJobKey);
            }
            
            // Delete the message after successful processing
            sqsService.deleteMessage(config.getWorkerOutputQueue(), message);
            
        } catch (Exception e) {
            logger.error("Failed to process worker message: {}", e.getMessage());
            // Don't delete - will retry after visibility timeout
        }
    }
    
    /**
     * Handles a success response from a worker
     * @return The inputFileS3Key if job is complete, null otherwise
     */
    private String handleSuccessResponse(Message message) throws Exception {
        WorkerSuccessResponse response = sqsService.parseMessage(message.body(), WorkerSuccessResponse.class);
        WorkerSuccessResponse.ResponseData data = response.getData();
        
        logger.info("Worker success: {} / {} -> {}", 
                data.getFileUrl(), data.getParsingMethod(), data.getOutputUrl());
        
        return jobTracker.recordSuccess(
                data.getFileUrl(),
                data.getParsingMethod(),
                data.getOutputUrl()
        );
    }
    
    /**
     * Handles an error response from a worker
     * @return The inputFileS3Key if job is complete, null otherwise
     */
    private String handleErrorResponse(Message message) throws Exception {
        WorkerErrorResponse response = sqsService.parseMessage(message.body(), WorkerErrorResponse.class);
        WorkerErrorResponse.ErrorData data = response.getData();
        
        logger.warn("Worker error: {} / {} - {}", 
                data.getFileUrl(), data.getParsingMethod(), data.getError());
        
        return jobTracker.recordError(
                data.getFileUrl(),
                data.getParsingMethod(),
                data.getError()
        );
    }
    
    /**
     * Handles job completion - generates summary and notifies local app
     */
    private void handleJobCompletion(String inputFileS3Key) {
        try {
            JobTracker.JobInfo jobInfo = jobTracker.getJob(inputFileS3Key);
            if (jobInfo == null) {
                logger.error("Job not found for completion: {}", inputFileS3Key);
                return;
            }
            
            // Generate HTML summary
            String html = htmlGenerator.generateSummary(jobInfo.getResults());
            
            // Upload to S3
            String summaryKey = "outputs/summary-" + System.currentTimeMillis() + ".html";
            String summaryS3Key = s3Service.uploadHtml(summaryKey, html);
            
            logger.info("Uploaded summary for job {} to {}", inputFileS3Key, summaryS3Key);
            
            // Send response to local application
            LocalAppResponse response = LocalAppResponse.taskComplete(inputFileS3Key, summaryKey);
            sqsService.sendMessage(config.getLocalAppOutputQueue(), response);
            
            logger.info("Sent completion message to local app for job {}", inputFileS3Key);
            
            // Remove the job from tracker
            jobTracker.removeJob(inputFileS3Key);
            
        } catch (Exception e) {
            logger.error("Failed to handle job completion for {}: {}", inputFileS3Key, e.getMessage());
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


