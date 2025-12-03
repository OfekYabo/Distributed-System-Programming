package com.distributed.systems;

import com.distributed.systems.shared.AwsClientFactory;
import com.distributed.systems.shared.model.LocalAppRequest;
import com.distributed.systems.shared.model.LocalAppResponse;
import com.distributed.systems.shared.service.Ec2Service;
import com.distributed.systems.shared.service.S3Service;
import com.distributed.systems.shared.service.SqsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.sqs.model.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Local Application main class
 * 
 * Usage: java -jar local-app.jar inputFileName outputFileName n [terminate]
 */
public class LocalApp {

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
        this.jobId = UUID.randomUUID().toString().substring(0, 8);

        // Initialize shared services using AwsClientFactory
        String region = config.getAwsRegion();
        this.ec2Service = new Ec2Service(AwsClientFactory.createEc2Client(region));
        this.s3Service = new S3Service(AwsClientFactory.createS3Client(region), config.getS3BucketName());
        this.sqsService = new SqsService(AwsClientFactory.createSqsClient(region));

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
            ensureQueuesExist();

            // Step 3: Ensure Manager is running
            logger.info("Step 2: Ensuring Manager is running...");
            ensureManagerRunning();

            // Step 4: Upload input file to S3
            logger.info("Step 3: Uploading input file to S3...");
            ensureBucketExists();
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

    private void ensureQueuesExist() {
        int visibilityTimeout = config.getVisibilityTimeout();
        int waitTime = config.getWaitTimeSeconds();

        sqsService.createQueueIfNotExists(config.getLocalAppInputQueue(), visibilityTimeout, waitTime);
        sqsService.createQueueIfNotExists(config.getLocalAppOutputQueue(), visibilityTimeout, waitTime);
        // Manager queues should be created by Manager, but we can ensure them here if
        // we want strictness.
        // For now, we only ensure the queues we interact with directly or expect to
        // exist.
        // Actually, we send to LocalAppInputQueue (Manager reads from it), and receive
        // from LocalAppOutputQueue.
        // So we should ensure they exist.
    }

    private void ensureManagerRunning() {
        String managerTagKey = "Role";
        String managerTagValue = "manager";

        if (ec2Service.isInstanceRunning(managerTagKey, managerTagValue)) {
            logger.info("Manager instance is already running.");
            return;
        }

        logger.info("No Manager running - launching new instance...");
        String userData = createManagerUserDataScript();

        List<Tag> tags = Arrays.asList(
                Tag.builder().key(managerTagKey).value(managerTagValue).build(),
                Tag.builder().key("Name").value("TextAnalysis-Manager").build());

        Instance instance = ec2Service.launchInstance(
                config.getManagerAmiId(),
                config.getManagerInstanceType(),
                userData,
                config.getManagerIamRole(),
                config.getManagerSecurityGroup(),
                config.getManagerKeyName(),
                tags,
                1, 1);

        logger.info("Manager instance launched: {}", instance.instanceId());

        // Wait for it to be running
        waitForInstanceRunning(managerTagKey, managerTagValue);
    }

    private void waitForInstanceRunning(String tagKey, String tagValue) {
        logger.info("Waiting for Manager to be in RUNNING state...");
        int maxAttempts = 60; // 5 minutes
        for (int i = 0; i < maxAttempts; i++) {
            if (ec2Service.isInstanceRunning(tagKey, tagValue)) {
                logger.info("Manager is running.");
                // Give it a bit more time to bootstrap
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        throw new RuntimeException("Timeout waiting for Manager to start");
    }

    private String createManagerUserDataScript() {
        // Read the local .env file content
        String envContent;
        try {
            envContent = new String(Files.readAllBytes(Paths.get(".env")));
        } catch (IOException e) {
            logger.warn("Could not read .env file, using empty config: {}", e.getMessage());
            envContent = "";
        }

        return "#!/bin/bash\n" +
                "set -e\n" +
                "exec > /var/log/user-data.log 2>&1\n" +
                "echo 'Starting Manager bootstrap...'\n" +
                "cd /home/ec2-user\n" +
                "# Install Java 11 (Amazon Corretto)\n" +
                "echo 'Installing Java...'\n" +
                "dnf install -y java-11-amazon-corretto-headless\n" +
                "# Download manager.jar from S3\n" +
                "echo 'Downloading manager.jar from S3...'\n" +
                "aws s3 cp s3://" + config.getS3BucketName() + "/manager.jar /home/ec2-user/manager.jar\n" +
                "# Create .env file with configuration\n" +
                "echo 'Creating .env file...'\n" +
                "cat <<'EOF' > .env\n" +
                envContent + "\n" +
                "EOF\n" +
                "# Run the Manager\n" +
                "echo 'Starting Manager...'\n" +
                "java -jar manager.jar >> /var/log/manager.log 2>&1 &\n" +
                "echo 'Manager started successfully'\n";
    }

    private void ensureBucketExists() {
        s3Service.ensureBucketExists();
    }

    private void uploadInputFile() {
        Path inputPath = Paths.get(inputFileName);
        inputS3Key = String.format("inputs/%s/input.txt", jobId);
        s3Service.uploadFile(inputPath, inputS3Key);
        logger.info("Input file uploaded to S3: {}", inputS3Key);
    }

    private void sendTaskRequest() {
        LocalAppRequest request = LocalAppRequest.newTask(inputS3Key, n);
        sqsService.sendMessage(config.getLocalAppInputQueue(), request);
        logger.info("Task request sent: {}", request);
    }

    private LocalAppResponse waitForResponse() {
        logger.info("Polling for response... (this may take a while)");
        long startTime = System.currentTimeMillis();
        int pollCount = 0;

        while (true) {
            List<Message> messages = sqsService.receiveMessages(config.getLocalAppOutputQueue(), 1,
                    config.getWaitTimeSeconds(), config.getVisibilityTimeout());

            for (Message message : messages) {
                try {
                    LocalAppResponse response = sqsService.parseMessage(message.body(), LocalAppResponse.class);

                    if (response.isTaskComplete() &&
                            response.getData() != null &&
                            inputS3Key.equals(response.getData().getInputFileS3Key())) {

                        sqsService.deleteMessage(config.getLocalAppOutputQueue(), message);
                        logger.info("Response received after {} seconds",
                                (System.currentTimeMillis() - startTime) / 1000);
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
            if (pollCount % 6 == 0) {
                logger.info("Still waiting... ({} minutes elapsed)", (System.currentTimeMillis() - startTime) / 60000);
            }
        }
    }

    private void downloadOutputFile(LocalAppResponse response) {
        String summaryS3Key = response.getData().getSummaryS3Key();
        Path outputPath = Paths.get(outputFileName);
        s3Service.downloadToFile(summaryS3Key, outputPath);
        logger.info("Output file saved to: {}", outputFileName);
    }

    private void sendTerminateMessage() {
        LocalAppRequest terminateRequest = LocalAppRequest.terminate();
        sqsService.sendMessage(config.getLocalAppInputQueue(), terminateRequest);
        logger.info("Terminate message sent to Manager");
    }

    private void closeServices() {
        try {
            ec2Service.close();
        } catch (Exception e) {
            logger.error("Error closing EC2", e);
        }
        try {
            s3Service.close();
        } catch (Exception e) {
            logger.error("Error closing S3", e);
        }
        try {
            sqsService.close();
        } catch (Exception e) {
            logger.error("Error closing SQS", e);
        }
    }

    public static void main(String[] args) {
        logger.info("=== Local Application Starting ===");
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
        } catch (NumberFormatException e) {
            System.err.println("Error: n must be a positive integer");
            System.exit(1);
            return;
        }
        if (n <= 0) {
             System.err.println("n must be positive");
             System.exit(1);
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
