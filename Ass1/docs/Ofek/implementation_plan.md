# Distributed System Programming - Assignment 1 Implementation Plan

## Goal Description
Implement a distributed text analysis system using AWS (EC2, S3, SQS). The system consists of a Local Application, a Manager, and Workers. It processes a list of text files, performs specified analyses (POS, Constituency, Dependency), and generates a summary HTML file.

## User Review Required
> [!IMPORTANT]
> **AWS Costs**: This application launches EC2 instances. Ensure to terminate the Manager (using the `terminate` argument) or manually stop instances to avoid unexpected costs.
> **Credentials**: Ensure AWS credentials are correctly configured in `~/.aws/credentials` or via environment variables.
> **Security**: The assignment mentions compressing jars with passwords for security, but for this implementation, we will focus on functionality first.

## Proposed Changes

### Shared / Common
- Define SQS queue names and S3 bucket names (or pass them as configuration).
- Define message formats (JSON is recommended for SQS messages).

### Local Application (`local-app`)
#### [MODIFY] `src/main/java/com/distributed/systems/LocalApp.java` (or similar)
- **Args Parsing**: `inputFileName`, `outputFileName`, `n`, `terminate`.
- **S3**: Upload `inputFileName`.
- **EC2**: Check if Manager is running. If not, start it (using `ami` and `user-data` script).
- **SQS**: Send "New Task" message with input file location.
- **SQS**: Poll for "Done" message.
- **S3**: Download summary file.
- **Termination**: If `terminate` is set, send "Terminate" message to Manager.

### Manager (`manager`)
#### [MODIFY] `src/main/java/com/distributed/systems/Manager.java` (or similar)
- **SQS**: Listen for messages from Local App and Workers.
- **Thread Pool**: Use a thread pool to handle messages in parallel.
- **State Management**: Track active workers and tasks.
- **Scaling**: Launch 1 worker for every `n` messages. Max 19 instances.
- **Aggregation**: Store results from workers. When all tasks for a job are done, create summary.
- **Termination**: Handle "Terminate" message (stop accepting new jobs, wait for current to finish, terminate workers, terminate self).

### Worker (`worker`)
#### [MODIFY] `src/main/java/com/distributed/systems/Worker.java` (or similar)
- **SQS**: Listen for "Analysis Task" messages.
- **S3**: Download text file.
- **NLP**: Use Stanford NLP to perform requested analysis (POS, CONSTITUENCY, DEPENDENCY).
- **S3**: Upload result file.
- **SQS**: Send "Task Done" message to Manager.
- **Error Handling**: Catch exceptions, upload error description, notify Manager.

## Verification Plan

### Automated Tests
- **Unit Tests**: Test NLP logic locally.
- **Integration Tests**:
    - Run Local App, Manager, and Worker locally (simulating AWS with local SQS/S3 if possible, or using real AWS dev resources).

### Manual Verification
1.  **Local Test**: Run all components on the local machine.
    ```bash
    # Terminal 1
    java -jar manager/target/manager.jar
    # Terminal 2
    java -jar worker/target/worker.jar
    # Terminal 3
    java -jar local-app/target/local-app.jar input-sample.txt output.html 1
    ```
2.  **Cloud Test**:
    - Deploy Manager and Worker jars to S3 (using `deploy.sh`).
    - Run Local App to trigger cloud deployment.
    - Verify EC2 instances are created.
    - Verify output HTML is generated and downloaded.
    - Verify S3 buckets and SQS queues are cleaned up (if implemented) or manually check them.
