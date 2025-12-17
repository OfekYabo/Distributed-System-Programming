package com.distributed.systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe job tracker for managing jobs and their tasks
 * Tracks which tasks belong to which job (by inputFileS3Key)
 * and maintains results for generating summary HTML
 */
public class JobTracker {

    private static final Logger logger = LoggerFactory.getLogger(JobTracker.class);

    // Map from inputFileS3Key to JobInfo
    private final ConcurrentHashMap<String, JobInfo> jobs;

    // Global counter for needed workers
    private final AtomicInteger globalNeededWorkers;

    public JobTracker() {
        this.jobs = new ConcurrentHashMap<>();
        this.globalNeededWorkers = new AtomicInteger(0);
    }

    /**
     * Registers a new job
     */
    /**
     * Registers a new job
     */
    public void registerJob(String inputFileS3Key, int totalTasks, int n, String replyQueueUrl, String jobId) {
        JobInfo jobInfo = new JobInfo(inputFileS3Key, totalTasks, n, replyQueueUrl, jobId);
        jobs.put(inputFileS3Key, jobInfo);

        // Calculate initial needed workers for this job
        int initialNeeded = (int) Math.ceil((double) totalTasks / n);
        jobInfo.setPrevNeededWorkers(initialNeeded);
        globalNeededWorkers.addAndGet(initialNeeded);

        logger.info("Registered job {} (ID: {}) with {} tasks (n={}, replyQueue={}). Needed workers: {}",
                inputFileS3Key, jobId, totalTasks, n, replyQueueUrl, initialNeeded);
    }

    /**
     * Records a successful task completion
     * 
     * @return The inputFileS3Key if job is complete, null otherwise
     */
    public String recordSuccess(String inputFileS3Key) {
        JobInfo job = jobs.get(inputFileS3Key);
        if (job == null) {
            logger.warn("Job not found for inputFileS3Key: {}", inputFileS3Key);
            return null;
        }

        job.incrementCompleted();
        updateWorkerNeed(job); // Scaling recalculation

        logger.debug("Recorded success for a task in job {}", inputFileS3Key);

        if (job.isComplete()) {
            logger.info("Job {} is complete ({} tasks)", inputFileS3Key, job.getTotalTasks());
            return inputFileS3Key;
        }

        return null;
    }

    /**
     * Records a failed task result
     */
    public String recordError(String inputFileS3Key) {
        JobInfo job = jobs.get(inputFileS3Key);
        if (job == null) {
            logger.warn("Job not found for inputFileS3Key: {}", inputFileS3Key);
            return null;
        }

        job.incrementCompleted(); // Count as completed even if failed (handled)
        updateWorkerNeed(job);

        logger.debug("Recorded error for a task in job {}", inputFileS3Key);

        if (job.isComplete()) {
            logger.info("Job {} is complete ({} tasks)", inputFileS3Key, job.getTotalTasks());
            return inputFileS3Key;
        }

        return null;
    }

    private void updateWorkerNeed(JobInfo jobInfo) {
        int done = jobInfo.getCompletedTasks();
        int remaining = jobInfo.getTotalTasks() - done;
        int newNeeded = (int) Math.ceil((double) remaining / jobInfo.getN());
        int delta = newNeeded - jobInfo.getPrevNeededWorkers();

        if (delta != 0) {
            globalNeededWorkers.addAndGet(delta);
            jobInfo.setPrevNeededWorkers(newNeeded);
            logger.debug("Job {} worker need changed by {}. New global needed workers: {}",
                    jobInfo.getInputFileS3Key(), delta, globalNeededWorkers.get());
        }
    }

    /**
     * Gets the job info for a completed job
     */
    public JobInfo getJob(String inputFileS3Key) {
        return jobs.get(inputFileS3Key);
    }

    public String findInputFileKeyByJobId(String jobId) {
        for (JobInfo job : jobs.values()) {
            if (job.getJobId().equals(jobId)) {
                return job.getInputFileS3Key();
            }
        }
        return null;
    }

    /**
     * Removes a job after processing is complete
     */
    public void removeJob(String inputFileS3Key) {
        JobInfo jobInfo = jobs.remove(inputFileS3Key);
        if (jobInfo != null) {
            // Adjust global needed workers if job was removed before all tasks were
            // completed
            int currentNeeded = jobInfo.getPrevNeededWorkers();
            if (currentNeeded > 0) {
                globalNeededWorkers.addAndGet(-currentNeeded);
            }
            logger.info("Removed job {}", inputFileS3Key);
        }
    }

    /**
     * Checks if there are any active jobs
     */
    public boolean hasActiveJobs() {
        return !jobs.isEmpty();
    }

    /**
     * Gets the number of active jobs
     */
    public int getActiveJobCount() {
        return jobs.size();
    }

    /**
     * Gets the total number of pending tasks across all jobs
     */
    public int getTotalPendingTasks() {
        return jobs.values().stream()
                .mapToInt(job -> job.getTotalTasks() - job.getCompletedTasks())
                .sum();
    }

    /**
     * Gets the current number of globally needed workers
     */
    public int getGlobalNeededWorkers() {
        return globalNeededWorkers.get();
    }

    /**
     * Gets the 'n' value (files per worker) - returns max n across all jobs
     */
    public int getMaxN() {
        return jobs.values().stream()
                .mapToInt(JobInfo::getN)
                .max()
                .orElse(1);
    }

    /**
     * Information about a single job
     */
    public static class JobInfo {
        private final String inputFileS3Key;
        private final int totalTasks;
        private final int n;
        private final String replyQueueUrl;
        private final String jobId;
        private final AtomicInteger completedTasks = new AtomicInteger(0);
        // Stateless: No list of results stored here anymore

        // Scaling tracking
        private int prevNeededWorkers = 0;

        public JobInfo(String inputFileS3Key, int totalTasks, int n, String replyQueueUrl, String jobId) {
            this.inputFileS3Key = inputFileS3Key;
            this.totalTasks = totalTasks;
            this.n = n;
            this.replyQueueUrl = replyQueueUrl;
            this.jobId = jobId;
        }

        public void setPrevNeededWorkers(int workers) {
            this.prevNeededWorkers = workers;
        }

        public int getPrevNeededWorkers() {
            return prevNeededWorkers;
        }

        public String getInputFileS3Key() {
            return inputFileS3Key;
        }

        public String getReplyQueueUrl() {
            return replyQueueUrl;
        }

        public String getJobId() {
            return jobId;
        }

        public int getTotalTasks() {
            return totalTasks;
        }

        public int getN() {
            return n;
        }

        public int getCompletedTasks() {
            return completedTasks.get();
        }

        public void incrementCompleted() {
            completedTasks.incrementAndGet();
        }

        public boolean isComplete() {
            return completedTasks.get() >= totalTasks;
        }
    }
}
