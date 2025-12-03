package com.distributed.systems;

import com.distributed.systems.model.LocalAppRequest;
import com.distributed.systems.model.LocalAppResponse;
import com.distributed.systems.service.Ec2Service;
import com.distributed.systems.service.S3Service;
import com.distributed.systems.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.model.Message;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * Local Application main class
 * 
 * Usage: java -jar local-app.jar inputFileName outputFileName n [terminate]
 * 
 * This application:
 * 1. Checks if a Manager is running on EC2, starts one if not
 * 2. Uploads the input file to S3
 * 3. Sends a task request to the Manager via SQS
 * 4. Waits for the response
 * 5. Downloads the output HTML and saves it locally
 * 6. Optionally sends a terminate message
 */
public class LocalApp {
    // TODO: "question 8" - Refactor services (S3, SQS, EC2) into a shared common
    // module to avoid duplication between LocalApp, Manager, and Worker

    private static final Logger logger = LoggerFactory.getLogger(LocalApp.class);

    private final LocalAppConfig config;
    private final Ec2Service ec2Service;
    private final S3Service s3Service;
    private final SqsService sqsService;

    // Job info
    private final String inputFileName;
    private final String outputFileName;
    private final int n;
    private final boolean terminate;

    // Generated job ID
    private final String jobId;
    private String inputS3Key;

    public LocalApp(String inputFileName, String outputFileName, int n, boolean terminate) {
        this.config = new LocalAppConfig();
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
        this.n = n;
        this.terminate = terminate;
        // TODO: "question 6" - Move Job ID creation to main() or pass as argument to
        // allow logging before instance creation
        this.jobId = UUID.randomUUID().toString().substring(0, 8);

        // Initialize services
        this.ec2Service = new Ec2Service(config);
        this.s3Service = new S3Service(config);
        this.sqsService = new SqsService(config);

        logger.info("Local Application initialized");
        logger.info("  Input file: {}", inputFileName);
        logger.info("  Output file: {}", outputFileName);
        logger.info("  N (files per worker): {}", n);
        logger.info("  Terminate: {}", terminate);
        logger.info("  Job ID: {}", jobId);
    }

    /**
     * Runs the local application
     */
    public void run() {
        try {
            // Step 1: Validate input file exists
            validateInputFile();

            // Step 2: Ensure queues exist
            logger.info("Step 1: Ensuring SQS queues exist...");
            sqsService.ensureQueuesExist();

            // Step 3: Ensure Manager is running
            logger.info("Step 2: Ensuring Manager is running...");
            ec2Service.ensureManagerRunning();

            // Step 4: Upload input file to S3
            logger.info("Step 3: Uploading input file to S3...");
            uploadInputFile();

            // Step 5: Send task request to Manager
            logger.info("Step 4: Sending task request to Manager...");
            sendTaskRequest();

            // Step 6: Wait for response
            logger.info("Step 5: Waiting for response from Manager...");
            LocalAppResponse response = waitForResponse();

            // Step 7: Download and save output file
            logger.info("Step 6: Downloading output file...");
            downloadOutputFile(response);

            // Step 8: Send terminate message if requested
            if (terminate) {
                logger.info("Step 7: Sending terminate message...");
                sendTerminateMessage();
            }

            logger.info("=== Job completed successfully! ===");
            logger.info("Output saved to: {}", outputFileName);

        } catch (Exception e) {
            logger.error("Job failed: {}", e.getMessage(), e);
            throw new RuntimeException("Job failed", e);
        } finally {
            closeServices();
        }
    }

    /**
     * Validates that the input file exists
     */
    private void validateInputFile() {
        Path inputPath = Paths.get(inputFileName);
        if (!Files.exists(inputPath)) {
            throw new RuntimeException("Input file not found: " + inputFileName);
        }
        if (!Files.isReadable(inputPath)) {
            throw new RuntimeException("Input file not readable: " + inputFileName);
        }
        logger.info("Input file validated: {}", inputFileName);
    }

    /**
     * Uploads the input file to S3
     */
    private void uploadInputFile() {
        Path inputPath = Paths.get(inputFileName);
        inputS3Key = String.format("inputs/%s/input.txt", jobId);

        s3Service.uploadFile(inputPath, inputS3Key);
        logger.info("Input file uploaded to S3: {}", inputS3Key);
    }

    /**
     * Sends a task request to the Manager
     */
    private void sendTaskRequest() {
        LocalAppRequest request = LocalAppRequest.newTask(inputS3Key, n);
        sqsService.sendMessage(config.getLocalAppInputQueue(), request);
        logger.info("Task request sent: {}", request);
    }

    /**
     * Waits for a response from the Manager
     */
    private LocalAppResponse waitForResponse() {
        logger.info("Polling for response... (this may take a while)");

        int pollCount = 0;
        long startTime = System.currentTimeMillis();

        while (true) {
            List<Message> messages = sqsService.receiveMessages(config.getLocalAppOutputQueue());

            for (Message message : messages) {
                try {
                    LocalAppResponse response = sqsService.parseMessage(message.body(), LocalAppResponse.class);

                    // Check if this response is for our job
                    if (response.isTaskComplete() &&
                            response.getData() != null &&
                            inputS3Key.equals(response.getData().getInputFileS3Key())) {

                        // Delete the message
                        sqsService.deleteMessage(config.getLocalAppOutputQueue(), message);

                        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
                        logger.info("Response received after {} seconds", elapsedSeconds);
                        return response;
                    } else {
                        // This message is not for us, leave it in queue
                        logger.debug("Received response for different job: {}",
                                response.getData() != null ? response.getData().getInputFileS3Key() : "null");
                    }

                } catch (Exception e) {
                    logger.warn("Failed to parse response message: {}", e.getMessage());
                }
            }

            pollCount++;
            if (pollCount % 6 == 0) { // Log every ~2 minutes (6 * 20 seconds)
                long elapsedMinutes = (System.currentTimeMillis() - startTime) / (60 * 1000);
                logger.info("Still waiting for response... ({} minutes elapsed)", elapsedMinutes);
            }
        }
    }

    /**
     * Downloads the output file from S3 and saves it locally
     */
    private void downloadOutputFile(LocalAppResponse response) {
        String summaryS3Key = response.getData().getSummaryS3Key();
        Path outputPath = Paths.get(outputFileName);

        s3Service.downloadToFile(summaryS3Key, outputPath);
        logger.info("Output file saved to: {}", outputFileName);
    }

    /**
     * Sends a terminate message to the Manager
     */
    private void sendTerminateMessage() {
        LocalAppRequest terminateRequest = LocalAppRequest.terminate();
        sqsService.sendMessage(config.getLocalAppInputQueue(), terminateRequest);
        logger.info("Terminate message sent to Manager");
    }

    /**
     * Closes all services
     */
    private void closeServices() {
        try {
            ec2Service.close();
        } catch (Exception e) {
            logger.error("Error closing EC2 service: {}", e.getMessage());
        }

        try {
            s3Service.close();
        } catch (Exception e) {
            logger.error("Error closing S3 service: {}", e.getMessage());
        }

        try {
            sqsService.close();
        } catch (Exception e) {
            logger.error("Error closing SQS service: {}", e.getMessage());
        }
    }

    /**
     * Main entry point
     */
    public static void main(String[] args) {
        logger.info("=== Local Application Starting ===");

        // Parse command line arguments
        if (args.length < 3) {
            System.err.println("Usage: java -jar local-app.jar inputFileName outputFileName n [terminate]");
            System.err.println();
            System.err.println("Arguments:");
            System.err.println("  inputFileName   - Path to the input file with URLs and analysis types");
            System.err.println("  outputFileName  - Path where the output HTML will be saved");
            System.err.println("  n               - Number of files per worker (worker ratio)");
            System.err.println("  terminate       - Optional: if present, terminate the Manager after job completion");
            System.exit(1);
        }

        String inputFileName = args[0];
        String outputFileName = args[1];

        int n;
        try {
            n = Integer.parseInt(args[2]);
            // TODO: "question 4" - Move this check outside the try-catch block to separate
            // parsing errors from validation errors
            if (n <= 0) {
                throw new NumberFormatException("n must be positive");
            }
        } catch (NumberFormatException e) {
            System.err.println("Error: n must be a positive integer");
            System.exit(1);
            return;
        }

        boolean terminate = args.length > 3 && "terminate".equalsIgnoreCase(args[3]);

        try {
            LocalApp app = new LocalApp(inputFileName, outputFileName, n, terminate);
            app.run();
        } catch (Exception e) {
            logger.error("Application failed: {}", e.getMessage());
            System.exit(1);
        }

        logger.info("=== Local Application Finished ===");
    }
}
