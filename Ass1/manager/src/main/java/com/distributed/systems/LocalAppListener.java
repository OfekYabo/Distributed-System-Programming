package com.distributed.systems;

import com.distributed.systems.model.LocalAppRequest;
import com.distributed.systems.model.WorkerTaskMessage;
import com.distributed.systems.service.S3Service;
import com.distributed.systems.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread that listens for messages from Local Applications
 * Handles new task requests and termination requests
 */
public class LocalAppListener implements Runnable {
    
    private static final Logger logger = LoggerFactory.getLogger(LocalAppListener.class);
    
    private final ManagerConfig config;
    private final SqsService sqsService;
    private final S3Service s3Service;
    private final JobTracker jobTracker;
    private final AtomicBoolean running;
    private final AtomicBoolean acceptingJobs;
    private final AtomicBoolean terminateRequested;
    
    public LocalAppListener(ManagerConfig config, 
                           SqsService sqsService, 
                           S3Service s3Service,
                           JobTracker jobTracker,
                           AtomicBoolean running,
                           AtomicBoolean acceptingJobs,
                           AtomicBoolean terminateRequested) {
        this.config = config;
        this.sqsService = sqsService;
        this.s3Service = s3Service;
        this.jobTracker = jobTracker;
        this.running = running;
        this.acceptingJobs = acceptingJobs;
        this.terminateRequested = terminateRequested;
    }
    
    @Override
    public void run() {
        logger.info("LocalAppListener started");
        
        while (running.get()) {
            try {
                // Poll for messages from local applications
                List<Message> messages = sqsService.receiveMessages(config.getLocalAppInputQueue());
                
                for (Message message : messages) {
                    if (!running.get()) {
                        break;
                    }
                    
                    processMessage(message);
                }
                
            } catch (Exception e) {
                logger.error("Error in LocalAppListener", e);
                sleep(5000);
            }
        }
        
        logger.info("LocalAppListener stopped");
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
                sqsService.deleteMessage(config.getLocalAppInputQueue(), message);
            }
            
        } catch (Exception e) {
            logger.error("Failed to process message from local app", e);
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
        sqsService.deleteMessage(config.getLocalAppInputQueue(), message);
        
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
            sqsService.deleteMessage(config.getLocalAppInputQueue(), message);
            return;
        }
        
        String inputFileS3Key = data.getInputFileS3Key();
        int n = data.getN() > 0 ? data.getN() : 1;
        
        logger.info("Processing new task: {} (n={})", inputFileS3Key, n);
        
        try {
            // Download the input file from S3
            List<String> lines = s3Service.downloadAsLines(inputFileS3Key);
            logger.info("Downloaded input file with {} lines", lines.size());
            
            // Parse the input file and create tasks
            List<JobTracker.TaskInfo> tasks = new ArrayList<>();
            
            for (String line : lines) {
                String[] parts = line.split("\t");
                if (parts.length >= 2) {
                    String parsingMethod = parts[0].trim();
                    String url = parts[1].trim();
                    
                    if (!parsingMethod.isEmpty() && !url.isEmpty()) {
                        tasks.add(new JobTracker.TaskInfo(url, parsingMethod));
                    }
                }
            }
            
            if (tasks.isEmpty()) {
                logger.warn("No valid tasks found in input file: {}", inputFileS3Key);
                sqsService.deleteMessage(config.getLocalAppInputQueue(), message);
                return;
            }
            
            // Register the job with the tracker
            jobTracker.registerJob(inputFileS3Key, tasks, n);
            
            // Send tasks to worker queue
            for (JobTracker.TaskInfo task : tasks) {
                WorkerTaskMessage taskMessage = WorkerTaskMessage.create(task.parsingMethod, task.url);
                sqsService.sendMessage(config.getWorkerInputQueue(), taskMessage);
            }
            
            logger.info("Sent {} tasks to worker queue for job {}", tasks.size(), inputFileS3Key);
            
            // Delete the original message
            sqsService.deleteMessage(config.getLocalAppInputQueue(), message);
            
        } catch (Exception e) {
            logger.error("Failed to process new task: {}", inputFileS3Key, e);
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


