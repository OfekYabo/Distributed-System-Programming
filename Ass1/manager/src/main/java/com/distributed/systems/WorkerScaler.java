package com.distributed.systems;

import com.distributed.systems.service.Ec2Service;
import com.distributed.systems.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread that manages worker instance scaling
 * Periodically checks queue depth and scales workers accordingly
 */
public class WorkerScaler implements Runnable {
    
    private static final Logger logger = LoggerFactory.getLogger(WorkerScaler.class);
    
    private final ManagerConfig config;
    private final SqsService sqsService;
    private final Ec2Service ec2Service;
    private final JobTracker jobTracker;
    private final AtomicBoolean running;
    private final AtomicBoolean terminateRequested;
    
    public WorkerScaler(ManagerConfig config,
                       SqsService sqsService,
                       Ec2Service ec2Service,
                       JobTracker jobTracker,
                       AtomicBoolean running,
                       AtomicBoolean terminateRequested) {
        this.config = config;
        this.sqsService = sqsService;
        this.ec2Service = ec2Service;
        this.jobTracker = jobTracker;
        this.running = running;
        this.terminateRequested = terminateRequested;
    }
    
    @Override
    public void run() {
        logger.info("WorkerScaler started (interval: {}s)", config.getScalingIntervalSeconds());
        
        while (running.get()) {
            try {
                if (terminateRequested.get()) {
                    // In termination mode - don't scale up, just monitor
                    logger.debug("Termination requested - skipping scaling");
                } else {
                    // Normal operation - check and scale workers
                    checkAndScale();
                }
                
                // Sleep for the configured interval
                sleep(config.getScalingIntervalSeconds() * 1000L);
                
            } catch (Exception e) {
                logger.error("Error in WorkerScaler", e);
                sleep(5000);
            }
        }
        
        logger.info("WorkerScaler stopped");
    }
    
    /**
     * Checks the current state and scales workers if needed
     */
    private void checkAndScale() {
        try {
            // Get current state
            int pendingMessages = sqsService.getApproximateMessageCount(config.getWorkerInputQueue());
            int currentWorkers = ec2Service.getRunningWorkerCount();
            int n = jobTracker.getMaxN();
            
            if (n <= 0) {
                n = 1; // Safety fallback
            }
            
            // Calculate required workers: ceil(pendingMessages / n)
            int requiredWorkers = (int) Math.ceil((double) pendingMessages / n);
            
            // Cap at max instances
            requiredWorkers = Math.min(requiredWorkers, config.getMaxWorkerInstances());
            
            logger.debug("Scaling check: pending={}, current={}, required={}, n={}", 
                    pendingMessages, currentWorkers, requiredWorkers, n);
            
            // Scale up if needed
            if (requiredWorkers > currentWorkers) {
                int toCreate = Math.min(
                        requiredWorkers - currentWorkers,
                        config.getMaxWorkerInstances() - currentWorkers
                );
                
                if (toCreate > 0) {
                    logger.info("Scaling up: creating {} worker(s) (current={}, required={})", 
                            toCreate, currentWorkers, requiredWorkers);
                    ec2Service.launchWorkers(toCreate);
                }
            }
            
            // Note: We don't scale down automatically during normal operation
            // Workers will naturally finish their tasks
            
        } catch (Exception e) {
            logger.error("Error during scaling check", e);
        }
    }
    
    /**
     * Initiates graceful shutdown of all workers
     * Called when termination is requested and all jobs are complete
     */
    public void terminateAllWorkers() {
        logger.info("Terminating all workers...");
        
        try {
            ec2Service.terminateAllWorkers();
            
            // Wait for workers to terminate
            ec2Service.waitForWorkersToTerminate(120); // 2 minute timeout
            
        } catch (Exception e) {
            logger.error("Error terminating workers", e);
        }
    }
    
    /**
     * Gets the current number of running workers
     */
    public int getRunningWorkerCount() {
        try {
            return ec2Service.getRunningWorkerCount();
        } catch (Exception e) {
            logger.error("Error getting worker count", e);
            return 0;
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


