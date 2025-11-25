package com.distributed.systems;

import com.distributed.systems.model.WorkerErrorMessage;
import com.distributed.systems.model.WorkerResponseMessage;
import com.distributed.systems.model.WorkerTaskMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

/**
 * Handles SQS message operations: receiving, sending, and deleting
 */
public class SqsMessageHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(SqsMessageHandler.class);
    
    private final SqsClient sqsClient;
    private final WorkerConfig config;
    private final ObjectMapper objectMapper;
    private final String inputQueueUrl;
    private final String outputQueueUrl;
    
    public SqsMessageHandler(SqsClient sqsClient, WorkerConfig config) {
        this.sqsClient = sqsClient;
        this.config = config;
        this.objectMapper = new ObjectMapper();
        
        // Get queue URLs
        this.inputQueueUrl = getQueueUrl(config.getInputQueueName());
        this.outputQueueUrl = getQueueUrl(config.getOutputQueueName());
        
        logger.info("SqsMessageHandler initialized with input queue: {}, output queue: {}", 
                inputQueueUrl, outputQueueUrl);
    }
    
    /**
     * Gets the queue URL from the queue name
     */
    private String getQueueUrl(String queueName) {
        try {
            GetQueueUrlRequest request = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            
            GetQueueUrlResponse response = sqsClient.getQueueUrl(request);
            return response.queueUrl();
        } catch (QueueDoesNotExistException e) {
            logger.error("Queue does not exist: {}", queueName, e);
            throw new RuntimeException("Queue not found: " + queueName, e);
        }
    }
    
    /**
     * Receives messages from the input queue
     * Uses long polling for efficiency
     */
    public List<Message> receiveMessages() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(inputQueueUrl)
                .maxNumberOfMessages(config.getMaxNumberOfMessages())
                .waitTimeSeconds(config.getWaitTimeSeconds())
                .visibilityTimeout(config.getVisibilityTimeoutSeconds())
                .build();
        
        ReceiveMessageResponse response = sqsClient.receiveMessage(request);
        List<Message> messages = response.messages();
        
        if (!messages.isEmpty()) {
            logger.info("Received {} message(s) from queue", messages.size());
        }
        
        return messages;
    }
    
    /**
     * Parses a message body into a WorkerTaskMessage
     */
    public WorkerTaskMessage parseTaskMessage(String messageBody) throws Exception {
        return objectMapper.readValue(messageBody, WorkerTaskMessage.class);
    }
    
    /**
     * Sends a success response message to the output queue
     */
    public void sendResponseMessage(WorkerResponseMessage message) {
        try {
            String messageBody = objectMapper.writeValueAsString(message);
            
            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(outputQueueUrl)
                    .messageBody(messageBody)
                    .build();
            
            SendMessageResponse response = sqsClient.sendMessage(request);
            logger.info("Sent response message to output queue. MessageId: {}", response.messageId());
        } catch (Exception e) {
            logger.error("Failed to send response message", e);
            throw new RuntimeException("Failed to send response message", e);
        }
    }
    
    /**
     * Sends an error message to the output queue
     */
    public void sendErrorMessage(WorkerErrorMessage message) {
        try {
            String messageBody = objectMapper.writeValueAsString(message);
            
            SendMessageRequest request = SendMessageRequest.builder()
                    .queueUrl(outputQueueUrl)
                    .messageBody(messageBody)
                    .build();
            
            SendMessageResponse response = sqsClient.sendMessage(request);
            logger.info("Sent error message to output queue. MessageId: {}", response.messageId());
        } catch (Exception e) {
            logger.error("Failed to send error message", e);
            throw new RuntimeException("Failed to send error message", e);
        }
    }
    
    /**
     * Deletes a message from the input queue
     * Should only be called after successful processing
     */
    public void deleteMessage(Message message) {
        try {
            DeleteMessageRequest request = DeleteMessageRequest.builder()
                    .queueUrl(inputQueueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            
            sqsClient.deleteMessage(request);
            logger.info("Deleted message from input queue. MessageId: {}", message.messageId());
        } catch (Exception e) {
            logger.error("Failed to delete message from input queue", e);
            // Don't throw exception - message will become visible again after visibility timeout
        }
    }
    
    /**
     * Closes the SQS client
     */
    public void close() {
        if (sqsClient != null) {
            sqsClient.close();
            logger.info("SQS client closed");
        }
    }
}

