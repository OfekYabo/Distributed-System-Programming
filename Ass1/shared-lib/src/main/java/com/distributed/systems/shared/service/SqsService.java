package com.distributed.systems.shared.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared service for SQS operations.
 * Decoupled from specific application configuration.
 */
public class SqsService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(SqsService.class);

    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final Map<String, String> queueUrlCache;

    public SqsService(SqsClient sqsClient) {
        this.sqsClient = sqsClient;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
        this.queueUrlCache = new ConcurrentHashMap<>();
    }

    /**
     * Gets the queue URL, caching it for future use.
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
     * Creates a queue if it doesn't exist.
     */
    public String createQueueIfNotExists(String queueName, int visibilityTimeout, int waitTimeSeconds) {
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
                            QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibilityTimeout),
                            QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, String.valueOf(waitTimeSeconds)))
                    .build();

            CreateQueueResponse createResponse = sqsClient.createQueue(createRequest);
            String queueUrl = createResponse.queueUrl();
            logger.info("Queue '{}' created", queueName);
            queueUrlCache.put(queueName, queueUrl);
            return queueUrl;
        }
    }

    /**
     * Sends a message to a queue.
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
     * Sends a raw string message to a queue.
     */
    public void sendRawMessage(String queueName, String messageBody) {
        String queueUrl = getQueueUrl(queueName);

        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        SendMessageResponse response = sqsClient.sendMessage(request);
        logger.debug("Sent message to queue {}. MessageId: {}", queueName, response.messageId());
    }

    /**
     * Receives messages from a queue.
     */
    public List<Message> receiveMessages(String queueName, int maxMessages, int waitTimeSeconds,
            int visibilityTimeout) {
        String queueUrl = getQueueUrl(queueName);

        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(maxMessages)
                .waitTimeSeconds(waitTimeSeconds)
                .visibilityTimeout(visibilityTimeout)
                .build();

        ReceiveMessageResponse response = sqsClient.receiveMessage(request);
        return response.messages();
    }

    /**
     * Gets the approximate number of messages in a queue.
     */
    public int getApproximateMessageCount(String queueName) {
        String queueUrl = getQueueUrl(queueName);

        GetQueueAttributesRequest request = GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                .build();

        GetQueueAttributesResponse response = sqsClient.getQueueAttributes(request);
        String count = response.attributes().get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
        return Integer.parseInt(count);
    }

    /**
     * Deletes a message from a queue.
     */
    public void deleteMessage(String queueName, Message message) {
        deleteMessage(queueName, message.receiptHandle());
    }

    /**
     * Deletes a message from a queue by receipt handle.
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
     * Parses a message body into the specified type.
     */
    public <T> T parseMessage(String messageBody, Class<T> clazz) {
        try {
            return objectMapper.readValue(messageBody, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse message", e);
        }
    }

    @Override
    public void close() {
        // Client lifecycle managed externally
        logger.debug("SqsService close called (client lifecycle managed externally)");
    }
}
