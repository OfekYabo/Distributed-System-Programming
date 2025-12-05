package com.distributed.systems;

import com.distributed.systems.service.Ec2Service;
import com.distributed.systems.service.S3Service;
import com.distributed.systems.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manager main class
 * Orchestrates all components: LocalAppListener, WorkerResultsListener, WorkerScaler
 * Handles lifecycle management and graceful shutdown
 */
public class Manager {
    
    private static final Logger logger = LoggerFactory.getLogger(Manager.class);
    
    private final ManagerConfig config;
    private final SqsService sqsService;
    private final S3Service s3Service;
    private final Ec2Service ec2Service;
    private final JobTracker jobTracker;
    private final HtmlSummaryGenerator htmlGenerator;
    
    private final AtomicBoolean running;
    private final AtomicBoolean acceptingJobs;
    private final AtomicBoolean terminateRequested;
    
    private ExecutorService executorService;
    private LocalAppListener localAppListener;
    private WorkerResultsListener workerResultsListener;
    private WorkerScaler workerScaler;
    
    private long lastActivityTime;
    
    public Manager(ManagerConfig config) {
        this.config = config;
        this.running = new AtomicBoolean(true);
        this.acceptingJobs = new AtomicBoolean(true);
        this.terminateRequested = new AtomicBoolean(false);
        
        // Initialize services
        this.sqsService = new SqsService(config);
        this.s3Service = new S3Service(config);
        this.ec2Service = new Ec2Service(config);
        
        // Initialize job tracker
        this.jobTracker = new JobTracker();
        
        // Initialize HTML generator
        this.htmlGenerator = new HtmlSummaryGenerator(s3Service);
        
        // Initialize threads
        this.localAppListener = new LocalAppListener(
                config, sqsService, s3Service, jobTracker, running, acceptingJobs, terminateRequested);
        
        this.workerResultsListener = new WorkerResultsListener(
                config, sqsService, s3Service, jobTracker, htmlGenerator, running);
        
        this.workerScaler = new WorkerScaler(
                config, sqsService, ec2Service, jobTracker, running, terminateRequested);
        
        this.lastActivityTime = System.currentTimeMillis();
        
        logger.info("Initialized (bucket: {}, region: {})", config.getS3BucketName(), config.getAwsRegion());
    }
    
    /**
     * Starts the manager
     */
    public void start() {
        logger.info("=== Manager Starting ===");
        
        // Ensure all SQS queues exist
        sqsService.ensureQueuesExist();
        
        // Setup shutdown hook
        setupShutdownHook();
        
        // Create thread pool for the three main threads
        executorService = Executors.newFixedThreadPool(3);
        
        // Start all threads
        executorService.submit(localAppListener);
        executorService.submit(workerResultsListener);
        executorService.submit(workerScaler);
        
        logger.info("All threads started");
        
        // Main monitoring loop
        monitorAndManage();
        
        // Cleanup
        shutdown();
    }
    
    /**
     * Main monitoring loop
     * Checks for termination conditions and idle timeout
     */
    private void monitorAndManage() {
        while (running.get()) {
            try {
                // Check if we should terminate
                if (shouldTerminate()) {
                    logger.info("Terminating...");
                    running.set(false);
                    break;
                }
                
                // Update activity time if there are active jobs
                if (jobTracker.hasActiveJobs()) {
                    lastActivityTime = System.currentTimeMillis();
                }
                
                // Log status periodically
                logStatus();
                
                // Sleep before next check
                Thread.sleep(10000); // Check every 10 seconds
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error in monitor loop: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Checks if manager should terminate
     */
    private boolean shouldTerminate() {
        // If terminate was requested
        if (terminateRequested.get()) {
            // Wait for all jobs to complete
            if (!jobTracker.hasActiveJobs()) {
                logger.info("Terminate requested and all jobs complete");
                return true;
            }
            logger.debug("Terminate requested but {} jobs still active", jobTracker.getActiveJobCount());
            return false;
        }
        
        // Check idle timeout
        long idleTimeMinutes = (System.currentTimeMillis() - lastActivityTime) / (60 * 1000);
        if (idleTimeMinutes >= config.getIdleTimeoutMinutes()) {
            logger.info("Idle timeout reached ({} minutes)", idleTimeMinutes);
            return true;
        }
        
        return false;
    }
    
    /**
     * Logs current status
     */
    private void logStatus() {
        int activeJobs = jobTracker.getActiveJobCount();
        int pendingTasks = jobTracker.getTotalPendingTasks();
        int runningWorkers = workerScaler.getRunningWorkerCount();
        
        logger.info("jobs={} pending={} workers={}", activeJobs, pendingTasks, runningWorkers);
    }
    
    /**
     * Graceful shutdown
     */
    private void shutdown() {
        logger.info("=== Manager Shutting Down ===");
        
        // Stop accepting new jobs
        acceptingJobs.set(false);
        
        // Signal threads to stop
        running.set(false);
        
        // Shutdown executor service
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }
        
        // Terminate all workers
        logger.info("Terminating all workers...");
        workerScaler.terminateAllWorkers();
        
        // Close services (except EC2 service which we need for self-termination)
        closeSqsAndS3Services();
        
        logger.info("=== Manager Stopped ===");
        
        // Self-terminate the manager's EC2 instance
        logger.info("Self-terminating manager EC2 instance...");
        ec2Service.terminateSelf();
        
        // Close EC2 service last
        try {
            ec2Service.close();
        } catch (Exception e) {
            logger.error("Error closing Ec2Service: {}", e.getMessage());
        }
    }
    
    /**
     * Closes SQS and S3 services (EC2 service is closed separately after self-termination)
     */
    private void closeSqsAndS3Services() {
        try {
            sqsService.close();
        } catch (Exception e) {
            logger.error("Error closing SqsService: {}", e.getMessage());
        }
        
        try {
            s3Service.close();
        } catch (Exception e) {
            logger.error("Error closing S3Service: {}", e.getMessage());
        }
    }
    
    /**
     * Sets up shutdown hook for graceful termination on SIGTERM/SIGINT
     */
    private void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            running.set(false);
        }));
    }
    
    /**
     * Main entry point
     */
    public static void main(String[] args) {
        logger.info("=== Manager Application Starting ===");
        
        try {
            ManagerConfig config = new ManagerConfig();
            Manager manager = new Manager(config);
            manager.start();
        } catch (Exception e) {
            logger.error("Fatal error in manager: {}", e.getMessage());
            System.exit(1);
        }
        
        logger.info("=== Manager Application Exited ===");
    }
}


