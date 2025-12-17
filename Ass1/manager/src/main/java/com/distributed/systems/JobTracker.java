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

    // Map from JobID to JobInfo
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
    public void registerJob(String jobId, int totalTasks, int n) {
        JobInfo jobInfo = new JobInfo(jobId, totalTasks, n);
        jobs.put(jobId, jobInfo);

        // Calculate initial needed workers for this job
        int initialNeeded = (int) Math.ceil((double) totalTasks / n);
        jobInfo.setPrevNeededWorkers(initialNeeded);
        globalNeededWorkers.addAndGet(initialNeeded);

        logger.info("Registered job ID: {} with {} tasks (n={}). Needed workers: {}",
                jobId, totalTasks, n, initialNeeded);
    }

    /**
     * Records a task completion (Success or Error)
     * 
     * @return The JobID if job is complete, null otherwise
     */
    public String recordTaskCompletion(String jobId) {
        JobInfo job = jobs.get(jobId);
        if (job == null) {
            logger.warn("Job not found for ID: {}", jobId);
            return null;
        }

        job.incrementCompleted();
        updateWorkerNeed(job); // Scaling recalculation

        logger.debug("Recorded task completion for job {}", jobId);

        if (job.isComplete()) {
            logger.info("Job {} is complete ({} tasks)", jobId, job.getTotalTasks());
            return jobId;
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
                    jobInfo.getJobId(), delta, globalNeededWorkers.get());
        }
    }

    public JobInfo getJob(String jobId) {
        return jobs.get(jobId);
    }

    public void removeJob(String jobId) {
        JobInfo jobInfo = jobs.remove(jobId);
        if (jobInfo != null) {
            int currentNeeded = jobInfo.getPrevNeededWorkers();
            if (currentNeeded > 0) {
                globalNeededWorkers.addAndGet(-currentNeeded);
            }
            logger.info("Removed job {}", jobId);
        }
    }

    public boolean hasActiveJobs() {
        return !jobs.isEmpty();
    }

    public int getActiveJobCount() {
        return jobs.size();
    }

    public int getGlobalNeededWorkers() {
        return globalNeededWorkers.get();
    }

    public int getMaxN() {
        return jobs.values().stream()
                .mapToInt(JobInfo::getN)
                .max()
                .orElse(1);
    }

    public int getTotalPendingTasks() {
        return jobs.values().stream()
                .mapToInt(job -> job.getTotalTasks() - job.getCompletedTasks())
                .sum();
    }

    /**
     * Information about a single job
     */
    public static class JobInfo {
        private final String jobId;
        private final int totalTasks;
        private final int n;
        private final AtomicInteger completedTasks = new AtomicInteger(0);

        private int prevNeededWorkers = 0;

        public JobInfo(String jobId, int totalTasks, int n) {
            this.jobId = jobId;
            this.totalTasks = totalTasks;
            this.n = n;
        }

        public void setPrevNeededWorkers(int workers) {
            this.prevNeededWorkers = workers;
        }

        public int getPrevNeededWorkers() {
            return prevNeededWorkers;
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
