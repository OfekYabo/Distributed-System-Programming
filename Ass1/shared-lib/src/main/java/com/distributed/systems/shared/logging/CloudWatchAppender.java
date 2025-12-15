package com.distributed.systems.shared.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Logback appender that sends logs to AWS CloudWatch Logs.
 * Batches log events and sends them asynchronously for better performance.
 */
public class CloudWatchAppender extends AppenderBase<ILoggingEvent> {

    private String logGroupName;
    private String logStreamName;
    private String region;
    private int batchSize = 100;
    private int flushIntervalMs = 5000;

    private Layout<ILoggingEvent> layout;
    private CloudWatchLogsClient cloudWatchLogsClient;
    private String sequenceToken;
    private final BlockingQueue<InputLogEvent> eventQueue = new LinkedBlockingQueue<>(10000);
    private ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public void setLogGroupName(String logGroupName) {
        this.logGroupName = logGroupName;
    }

    public void setLogStreamName(String logStreamName) {
        this.logStreamName = logStreamName;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setFlushIntervalMs(int flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }

    @Override
    public void start() {
        if (logGroupName == null || logGroupName.isEmpty()) {
            addError("logGroupName is required");
            System.err.println("[CloudWatchAppender] ERROR: logGroupName is required");
            return;
        }

        // Auto-generate log stream name with instance ID or hostname
        if (logStreamName == null || logStreamName.isEmpty()) {
            logStreamName = generateLogStreamName();
        }

        // Get region - prefer environment variable, then config, then default
        String awsRegion = System.getenv("AWS_REGION");
        if (awsRegion == null || awsRegion.isEmpty()) {
            // Check if config region is set and not a placeholder
            if (region != null && !region.isEmpty() && !region.startsWith("${")) {
                awsRegion = region;
            } else {
                awsRegion = "us-east-1";
            }
        }

        System.out.println("[CloudWatchAppender] Initializing with region=" + awsRegion + 
                           ", logGroup=" + logGroupName + ", logStream=" + logStreamName);

        try {
            cloudWatchLogsClient = CloudWatchLogsClient.builder()
                    .region(Region.of(awsRegion))
                    .build();

            ensureLogGroupExists();
            ensureLogStreamExists();

            running.set(true);
            
            // Start background flush thread
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "cloudwatch-log-flusher");
                t.setDaemon(true);
                return t;
            });
            scheduler.scheduleAtFixedRate(this::flushLogs, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

            super.start();
            String msg = "CloudWatch appender started - group: " + logGroupName + ", stream: " + logStreamName;
            addInfo(msg);
            System.out.println("[CloudWatchAppender] " + msg);

        } catch (Exception e) {
            addError("Failed to initialize CloudWatch appender", e);
            System.err.println("[CloudWatchAppender] ERROR: Failed to initialize - " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    @Override
    public void stop() {
        running.set(false);
        
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        // Final flush
        flushLogs();

        if (cloudWatchLogsClient != null) {
            cloudWatchLogsClient.close();
        }
        
        super.stop();
    }

    @Override
    protected void append(ILoggingEvent event) {
        if (!running.get()) {
            return;
        }

        try {
            String message = layout != null ? layout.doLayout(event) : event.getFormattedMessage();
            
            InputLogEvent logEvent = InputLogEvent.builder()
                    .timestamp(event.getTimeStamp())
                    .message(message)
                    .build();

            // Non-blocking offer - drop if queue is full
            if (!eventQueue.offer(logEvent)) {
                addWarn("CloudWatch log queue full, dropping message");
            }

            // Flush if batch size reached
            if (eventQueue.size() >= batchSize) {
                scheduler.submit(this::flushLogs);
            }

        } catch (Exception e) {
            addError("Error queuing log event", e);
        }
    }

    private synchronized void flushLogs() {
        if (eventQueue.isEmpty()) {
            return;
        }

        List<InputLogEvent> batch = new ArrayList<>();
        eventQueue.drainTo(batch, batchSize);

        if (batch.isEmpty()) {
            return;
        }

        // Sort by timestamp (required by CloudWatch)
        batch.sort((a, b) -> Long.compare(a.timestamp(), b.timestamp()));

        try {
            PutLogEventsRequest.Builder requestBuilder = PutLogEventsRequest.builder()
                    .logGroupName(logGroupName)
                    .logStreamName(logStreamName)
                    .logEvents(batch);

            if (sequenceToken != null) {
                requestBuilder.sequenceToken(sequenceToken);
            }

            PutLogEventsResponse response = cloudWatchLogsClient.putLogEvents(requestBuilder.build());
            sequenceToken = response.nextSequenceToken();

        } catch (InvalidSequenceTokenException e) {
            // Get the expected token and retry
            sequenceToken = e.expectedSequenceToken();
            try {
                PutLogEventsResponse response = cloudWatchLogsClient.putLogEvents(
                        PutLogEventsRequest.builder()
                                .logGroupName(logGroupName)
                                .logStreamName(logStreamName)
                                .logEvents(batch)
                                .sequenceToken(sequenceToken)
                                .build());
                sequenceToken = response.nextSequenceToken();
            } catch (Exception retryEx) {
                addError("Failed to send logs after token refresh", retryEx);
                // Re-queue events
                eventQueue.addAll(batch);
            }
        } catch (DataAlreadyAcceptedException e) {
            sequenceToken = e.expectedSequenceToken();
        } catch (Exception e) {
            addError("Failed to send logs to CloudWatch", e);
            // Re-queue events for retry (up to a limit)
            if (eventQueue.size() < 5000) {
                eventQueue.addAll(batch);
            }
        }
    }

    private void ensureLogGroupExists() {
        try {
            System.out.println("[CloudWatchAppender] Creating log group: " + logGroupName);
            cloudWatchLogsClient.createLogGroup(
                    CreateLogGroupRequest.builder()
                            .logGroupName(logGroupName)
                            .build());
            String msg = "Created log group: " + logGroupName;
            addInfo(msg);
            System.out.println("[CloudWatchAppender] " + msg);
        } catch (ResourceAlreadyExistsException e) {
            System.out.println("[CloudWatchAppender] Log group already exists: " + logGroupName);
            // Log group already exists - that's fine
        } catch (Exception e) {
            String msg = "Could not create log group: " + e.getMessage();
            addWarn(msg);
            System.err.println("[CloudWatchAppender] WARNING: " + msg);
            e.printStackTrace(System.err);
        }
    }

    private void ensureLogStreamExists() {
        try {
            System.out.println("[CloudWatchAppender] Creating log stream: " + logStreamName);
            cloudWatchLogsClient.createLogStream(
                    CreateLogStreamRequest.builder()
                            .logGroupName(logGroupName)
                            .logStreamName(logStreamName)
                            .build());
            String msg = "Created log stream: " + logStreamName;
            addInfo(msg);
            System.out.println("[CloudWatchAppender] " + msg);
        } catch (ResourceAlreadyExistsException e) {
            System.out.println("[CloudWatchAppender] Log stream already exists: " + logStreamName);
            // Log stream already exists - get the sequence token
            try {
                DescribeLogStreamsResponse response = cloudWatchLogsClient.describeLogStreams(
                        DescribeLogStreamsRequest.builder()
                                .logGroupName(logGroupName)
                                .logStreamNamePrefix(logStreamName)
                                .build());
                
                response.logStreams().stream()
                        .filter(s -> s.logStreamName().equals(logStreamName))
                        .findFirst()
                        .ifPresent(stream -> sequenceToken = stream.uploadSequenceToken());
            } catch (Exception describeEx) {
                addWarn("Could not get sequence token: " + describeEx.getMessage());
            }
        } catch (Exception e) {
            String msg = "Could not create log stream: " + e.getMessage();
            addError(msg, e);
            System.err.println("[CloudWatchAppender] ERROR: " + msg);
            e.printStackTrace(System.err);
        }
    }

    private String generateLogStreamName() {
        // Try to get EC2 instance ID
        String instanceId = System.getenv("EC2_INSTANCE_ID");
        if (instanceId != null && !instanceId.isEmpty()) {
            return instanceId;
        }

        // Try hostname
        try {
            String hostname = java.net.InetAddress.getLocalHost().getHostName();
            return hostname + "-" + System.currentTimeMillis();
        } catch (Exception e) {
            return "instance-" + System.currentTimeMillis();
        }
    }
}

