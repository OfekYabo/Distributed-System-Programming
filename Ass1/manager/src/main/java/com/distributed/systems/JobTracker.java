package com.distributed.systems;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    // Map from (url + parsingMethod) to inputFileS3Key for reverse lookup
    private final ConcurrentHashMap<String, String> taskToJobMapping;

    public JobTracker() {
        this.jobs = new ConcurrentHashMap<>();
        this.taskToJobMapping = new ConcurrentHashMap<>();
    }

    /**
     * Creates a task key from url and parsing method
     */
    private String createTaskKey(String url, String parsingMethod) {
        return parsingMethod + "|" + url;
    }

    /**
     * Registers a new job
     */
    public void registerJob(String inputFileS3Key, List<TaskInfo> tasks, int n) {
        JobInfo jobInfo = new JobInfo(inputFileS3Key, tasks.size(), n);
        jobs.put(inputFileS3Key, jobInfo);

        // Register all tasks for reverse lookup
        for (TaskInfo task : tasks) {
            String taskKey = createTaskKey(task.url, task.parsingMethod);
            taskToJobMapping.put(taskKey, inputFileS3Key);
            jobInfo.addPendingTask(taskKey);
        }

        logger.info("Registered job {} with {} tasks (n={})", inputFileS3Key, tasks.size(), n);
    }

    /**
     * Records a successful task completion
     * 
     * @return The inputFileS3Key if job is complete, null otherwise
     */
    public String recordSuccess(String url, String parsingMethod, String outputS3Url) {
        String taskKey = createTaskKey(url, parsingMethod);
        String inputFileS3Key = taskToJobMapping.get(taskKey);

        if (inputFileS3Key == null) {
            logger.warn("Received result for unknown task: {} / {}", url, parsingMethod);
            return null;
        }

        JobInfo jobInfo = jobs.get(inputFileS3Key);
        if (jobInfo == null) {
            logger.warn("Job not found for task: {}", inputFileS3Key);
            return null;
        }

        TaskResult result = new TaskResult(url, parsingMethod, outputS3Url, null, true);
        boolean isComplete = jobInfo.recordResult(result);

        logger.debug("Recorded success for task {} / {} in job {}", url, parsingMethod, inputFileS3Key);

        if (isComplete) {
            logger.info("Job {} is complete ({} tasks)", inputFileS3Key, jobInfo.getTotalTasks());
            return inputFileS3Key;
        }

        return null;
    }

    /**
     * Records a failed task (after worker exhausted retries)
     * 
     * @return The inputFileS3Key if job is complete, null otherwise
     */
    public String recordError(String url, String parsingMethod, String error) {
        String taskKey = createTaskKey(url, parsingMethod);
        String inputFileS3Key = taskToJobMapping.get(taskKey);

        if (inputFileS3Key == null) {
            logger.warn("Received error for unknown task: {} / {}", url, parsingMethod);
            return null;
        }

        JobInfo jobInfo = jobs.get(inputFileS3Key);
        if (jobInfo == null) {
            logger.warn("Job not found for task: {}", inputFileS3Key);
            return null;
        }

        TaskResult result = new TaskResult(url, parsingMethod, null, error, false);
        boolean isComplete = jobInfo.recordResult(result);

        logger.debug("Recorded error for task {} / {} in job {}: {}", url, parsingMethod, inputFileS3Key, error);

        if (isComplete) {
            logger.info("Job {} is complete ({} tasks)", inputFileS3Key, jobInfo.getTotalTasks());
            return inputFileS3Key;
        }

        return null;
    }

    /**
     * Gets the job info for a completed job
     */
    public JobInfo getJob(String inputFileS3Key) {
        return jobs.get(inputFileS3Key);
    }

    /**
     * Removes a job after processing is complete
     */
    public void removeJob(String inputFileS3Key) {
        JobInfo jobInfo = jobs.remove(inputFileS3Key);
        if (jobInfo != null) {
            // Clean up task mappings
            for (TaskResult result : jobInfo.getResults()) {
                String taskKey = createTaskKey(result.url, result.parsingMethod);
                taskToJobMapping.remove(taskKey);
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
        private final AtomicInteger completedTasks;
        private final List<TaskResult> results;
        private final Set<String> pendingTasks;

        public JobInfo(String inputFileS3Key, int totalTasks, int n) {
            this.inputFileS3Key = inputFileS3Key;
            this.totalTasks = totalTasks;
            this.n = n;
            this.completedTasks = new AtomicInteger(0);
            this.results = Collections.synchronizedList(new ArrayList<>());
            this.pendingTasks = Collections.synchronizedSet(new HashSet<>());
        }

        void addPendingTask(String taskKey) {
            pendingTasks.add(taskKey);
        }

        boolean recordResult(TaskResult result) {
            String taskKey = result.parsingMethod + "|" + result.url;
            if (pendingTasks.remove(taskKey)) {
                results.add(result);
                return completedTasks.incrementAndGet() >= totalTasks;
            }
            return false;
        }

        public String getInputFileS3Key() {
            return inputFileS3Key;
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

        public List<TaskResult> getResults() {
            return new ArrayList<>(results);
        }

        public boolean isComplete() {
            return completedTasks.get() >= totalTasks;
        }
    }

    /**
     * Information about a task to be processed
     */
    public static class TaskInfo {
        public final String url;
        public final String parsingMethod;

        public TaskInfo(String url, String parsingMethod) {
            this.url = url;
            this.parsingMethod = parsingMethod;
        }
    }

    /**
     * Result of a task (success or error)
     */
    public static class TaskResult {
        public final String url;
        public final String parsingMethod;
        public final String outputS3Url;
        public final String error;
        public final boolean success;

        public TaskResult(String url, String parsingMethod, String outputS3Url, String error, boolean success) {
            this.url = url;
            this.parsingMethod = parsingMethod;
            this.outputS3Url = outputS3Url;
            this.error = error;
            this.success = success;
        }

        public boolean isSuccess() {
            return success;
        }

        public String getOutputUrl() {
            return outputS3Url;
        }
    }
}
