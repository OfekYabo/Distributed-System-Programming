package com.distributed.systems.service;

import com.distributed.systems.LocalAppConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service for SQS operations
 * Handles message sending, receiving, and queue management for Local Application
 */
public class SqsService implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(SqsService.class);
    
    private final SqsClient sqsClient;
    private final LocalAppConfig config;
    private final ObjectMapper objectMapper;
    private final Map<String, String> queueUrlCache;
    
    public SqsService(LocalAppConfig config) {
        this.config = config;
        this.objectMapper = new ObjectMapper();
        this.queueUrlCache = new ConcurrentHashMap<>();
        
        this.sqsClient = SqsClient.builder()
                .region(Region.of(config.getAwsRegion()))
                .build();
        
        logger.info("SQS Service initialized (region: {})", config.getAwsRegion());
    }
    
    /**
     * Gets the queue URL, caching it for future use
     */
    public String getQueueUrl(String queueName) {
        return queueUrlCache.computeIfAbsent(queueName, name -> {
            try {
                GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                        .queueName(name)
                        .build();
                
                GetQueueUrlResponse response = sqsClient.getQueueUrl(request);
                logger.debug("Resolved queue URL for {}: {}", name, response.queueUrl());
                return response.queueUrl();
            } catch (QueueDoesNotExistException e) {
                logger.error("Queue does not exist: {}", name);
                throw new RuntimeException("Queue not found: " + name, e);
            }
        });
    }
    
    /**
     * Creates a queue if it doesn't exist
     * @param queueName the name of the queue to create
     * @return the queue URL
     */
    public String createQueueIfNotExists(String queueName) {
        try {
            // First check if queue already exists
            GetQueueUrlRequest getRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            
            GetQueueUrlResponse getResponse = sqsClient.getQueueUrl(getRequest);
            String queueUrl = getResponse.queueUrl();
            logger.info("Queue '{}' exists", queueName);
            queueUrlCache.put(queueName, queueUrl);
            return queueUrl;
            
        } catch (QueueDoesNotExistException e) {
            // Queue doesn't exist, create it
            logger.info("Creating queue '{}'...", queueName);
            
            CreateQueueRequest createRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(Map.of(
                            QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(config.getVisibilityTimeoutSeconds()),
                            QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, String.valueOf(config.getWaitTimeSeconds())
                    ))
                    .build();
            
            CreateQueueResponse createResponse = sqsClient.createQueue(createRequest);
            String queueUrl = createResponse.queueUrl();
            logger.info("Queue '{}' created", queueName);
            queueUrlCache.put(queueName, queueUrl);
            return queueUrl;
        }
    }
    
    /**
     * Ensures all required queues exist, creating them if necessary
     */
    public void ensureQueuesExist() {
        logger.info("Checking SQS queues...");
        
        createQueueIfNotExists(config.getLocalAppInputQueue());
        createQueueIfNotExists(config.getLocalAppOutputQueue());
        
        logger.info("All queues ready");
    }
    
    /**
     * Sends a message to a queue
     */
    public void sendMessage(String queueName, Object message) {
        try {
            String messageBody = objectMapper.writeValueAsString(message);
            sendRawMessage(queueName, messageBody);
        } catch (Exception e) {
            logger.error("Failed to serialize message: {}", e.getMessage());
            throw new RuntimeException("Failed to send message", e);
        }
    }
    
    /**
     * Sends a raw string message to a queue
     */
    public void sendRawMessage(String queueName, String messageBody) {
        String queueUrl = getQueueUrl(queueName);
        
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();
        
        SendMessageResponse response = sqsClient.sendMessage(request);
        logger.info("Sent message to queue {}. MessageId: {}", queueName, response.messageId());
    }
    
    /**
     * Receives messages from a queue with long polling
     */
    public List<Message> receiveMessages(String queueName) {
        return receiveMessages(queueName, config.getMaxNumberOfMessages());
    }
    
    /**
     * Receives messages from a queue with specified max count
     */
    public List<Message> receiveMessages(String queueName, int maxMessages) {
        String queueUrl = getQueueUrl(queueName);
        
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(config.getWaitTimeSeconds())
                .visibilityTimeout(config.getVisibilityTimeoutSeconds())
                .build();
        
        ReceiveMessageResponse response = sqsClient.receiveMessage(request);
        List<Message> messages = response.messages();
        
        if (!messages.isEmpty()) {
            logger.debug("Received {} message(s) from queue {}", messages.size(), queueName);
        }
        
        return messages;
    }
    
    /**
     * Deletes a message from a queue
     */
    public void deleteMessage(String queueName, Message message) {
        deleteMessage(queueName, message.receiptHandle());
    }
    
    /**
     * Deletes a message from a queue by receipt handle
     */
    public void deleteMessage(String queueName, String receiptHandle) {
        String queueUrl = getQueueUrl(queueName);
        
        DeleteMessageRequest request = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();
        
        sqsClient.deleteMessage(request);
        logger.debug("Deleted message from queue {}", queueName);
    }
    
    /**
     * Parses a message body into the specified type
     */
    public <T> T parseMessage(String messageBody, Class<T> clazz) throws Exception {
        return objectMapper.readValue(messageBody, clazz);
    }
    
    /**
     * Serializes an object to JSON string
     */
    public String toJson(Object obj) throws Exception {
        return objectMapper.writeValueAsString(obj);
    }
    
    @Override
    public void close() {
        if (sqsClient != null) {
            sqsClient.close();
            logger.info("SQS Service closed");
        }
    }
}

