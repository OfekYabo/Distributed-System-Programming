# Distributed Text Analysis System
**Authors:** Ofek Yabo
**Course:** Distributed Systems Programming - Assignment 1

## Overview
A scalable, distributed system for processing large-scale text analysis tasks on AWS. The system parses text files from URLs using Stanford NLP models (POS Tagging, Constituency Parsing, Dependency Parsing) and aggregates results. It is built to be robust, fault-tolerant, and dynamic.

## Architecture
The system consists of three main components:

1.  **Local Application (Client)**:
    -   Entry point for the user.
    -   Launcher: Checks for an active **Manager** on EC2. If not found, it starts one using a [User Data Script](local-app/src/main/java/com/distributed/systems/LocalApp.java#L249) that installs Java and runs the manager JAR.
    -   Communicates with the Manager via SQS (`localApp-to-manager-requests`).
    -   Waits for the final summary HTML on a unique reply queue.

2.  **Manager (Coordinator)**:
    -   **Orchestrator**: Runs on a permanent EC2 instance.
    -   **Job Tracker**: Manages multiple concurrent jobs. Calculates the *global* worker need by summing the requirements of all active jobs.
    -   **Auto-Scaler**: Monitors the global workload and dynamically launches/terminates **Worker** instances.
    -   **Aggregator**: Collects results from Workers, stores them in S3, and generates a final HTML summary.
    -   **Fault Tolerance**: Relies on SQS **Visibility Timeout**. If a worker crashes or fails to delete a message within the timeout (1000s), SQS automatically makes the task visible again for another worker to pick up.

3.  **Worker (Node)**:
    -   **Processor**: Runs on transient EC2 instances.
    -   **Stateless**: Pulls tasks from SQS, processes them using Stanford NLP, and uploads results to S3.
    -   **Robustness**: Handles large files by spilling to disk if RAM usage > 50MB.

## Concurrency & Threading Model
The system uses multi-threading to handle asynchronous operations efficiently.

### 1. Manager (4 Threads)
The Manager uses a `FixedThreadPool` of 3 threads plus the main thread:
*   **Main Thread**: Runs the `monitorAndManage()` loop, checking for system-wide idle timeouts and termination requests.
*   **LocalAppListener Thread**: Polling thread that listens for new Job requests from Local Apps.
*   **WorkerResultsListener Thread**: Polling thread that listens for completed task messages from Workers and updates the `JobTracker`.
*   **WorkerScaler Thread**: Periodic thread (runs every 30s) that checks `JobTracker.getGlobalNeededWorkers()` vs. `EC2.getRunningInstances()` and scales up/down.

### 2. Worker (2 Threads)
*   **Main Thread**: Continuously polls SQS for new tasks.
*   **TaskExecutor Thread**: A `SingleThreadExecutor` used to run the actual parsing logic (`TaskProcessor`) with a strict timeout. If the parser hangs, the Main Thread can kill the executor and restart it (Nuclear Option) to keep the Worker alive.

### 3. Local App (1 Thread)
*   **Main Thread**: executes the workflow sequentially: Validate -> Launch Manager -> Upload -> Send Request -> Poll for Reply.

## Workflow Dynamics
1.  **Submission**: Client sends a job (Input File + N workers ratio).
2.  **Global Scaling**:
    *   For each job, the [JobTracker](manager/src/main/java/com/distributed/systems/JobTracker.java#L70) calculates: `Needed = Ceil(RemainingTasks / N)`.
    *   The Scaler sums this value across **all** active jobs to determine the `GlobalNeededWorkers`.
    *   It then launches/terminates instances to match this number (up to `MAX_WORKER_INSTANCES`).
3.  **Processing**: Workers pull tasks. They use a "Hybrid Stream" approach—buffering in RAM up to 50MB, then spilling to disk.
4.  **Completion**: Once a job's task count reaches 0, the Manager compiles the summary.

## Configuration (.env)

### AWS & Resources
*   `AWS_REGION`: `us-east-1`
*   `S3_BUCKET_NAME`: `ds-assignment-1-ofek`

### Queues (SQS)
*   `LOCAL_APP_INPUT_QUEUE`: `localApp-to-manager-requests`
*   `WORKER_INPUT_QUEUE`: `manager-to-workers-tasks`
*   `WORKER_OUTPUT_QUEUE`: `workers-to-manager-results`
*   `VISIBILITY_TIMEOUT_SECONDS`: `1000` (Safety net for crashed workers).
*   `WAIT_TIME_SECONDS`: `20` (Long polling).

### Performance
*   `MANAGER_INSTANCE_TYPE`: `t2.micro`
*   `WORKER_INSTANCE_TYPE`: `t3.medium` (4GB RAM for NLP models).
*   `MAX_WORKER_INSTANCES`: `18`
*   `SCALING_INTERVAL_SECONDS`: `30`

### Logic
*   `WORKER_MAX_MESSAGES`: `1` (Process one task at a time).
*   `WORKER_MAX_SENTENCE_LENGTH`: `20` (Skips sentences longer than 20 words during Constituency Parsing to prevent timeouts).
*   `TEMP_DIR`: `/tmp/worker`

### Worker Logic
*   `WORKER_MAX_MESSAGES`: `1` (Process one task at a time).
*   `WORKER_MAX_SENTENCE_LENGTH`: `20` (**Note**: This limits Constituency Parsing to sentences under 20 *words*. Increase if deeper analysis of long sentences is required, e.g., to 80).
*   `TEMP_DIR`: `/tmp/worker`

## Design Considerations

### Memory Efficiency & Scalability
The Manager is designed to handle **multiple concurrent clients** and **thousands of tasks** with minimal memory footprint:
1.  **Stateless Job Tracking**: The `JobTracker` in the Manager does **not** store the list of tasks or their results in memory while the job is running. It only maintains lightweight `AtomicInteger` counters (Total vs. Completed).
2.  **S3 Offloading**: Workers upload their results (and metadata) directly to S3. The Manager does not receive large text payloads. It only receives a lightweight notification message via SQS.
3.  **On-Demand Aggregation**: Only when a job reaches 100% completion does the Manager list the result files from S3 to generate the summary. This ensures that memory usage during the processing phase remains constant (O(1)) per job, regardless of the number of tasks.

### Why SQS?
We chose AWS SQS for communication to ensure **decoupling** and **load balancing**:
*   **Buffer & Flow Control**: SQS acts as a buffer between the fast Task Generator (Manager) and the slower Task Processors (Workers). It prevents the Manager from overwhelming workers.
*   **Automatic Load Balancing**: Workers "pull" tasks when they are ready. Faster workers process more tasks automatically without complex logic in the Manager.
*   **Fault Tolerance (Visibility Timeout)**: If a Worker crashes while processing a task (or if the EC2 instance is terminated), it fails to delete the message. After the `VISIBILITY_TIMEOUT_SECONDS` (1000s), SQS automatically makes the task visible again, allowing another health worker to pick it up.
*   **Separation of Concerns**:
    *   `manager-to-workers-tasks`: Regular work.
    *   `worker-control-queue`: High-priority signals (e.g., Termination) that can bypass the backlog of regular tasks.

### Worker Logic
*   `WORKER_MAX_MESSAGES`: `1` (Process one task at a time).
*   `WORKER_MAX_SENTENCE_LENGTH`: `20` (**Note**: This limits Constituency Parsing to sentences under 20 *words*. Increase if deeper analysis of long sentences is required, e.g., to 80).
*   `TEMP_DIR`: `/tmp/worker`

## Design Considerations

### Memory Efficiency & Scalability
The Manager is designed to handle **multiple concurrent clients** and **thousands of tasks** with minimal memory footprint:
1.  **Stateless Job Tracking**: The `JobTracker` in the Manager does **not** store the list of tasks or their results in memory while the job is running. It only maintains lightweight `AtomicInteger` counters (Total vs. Completed).
2.  **S3 Offloading**: Workers upload their results (and metadata) directly to S3. The Manager does not receive large text payloads. It only receives a lightweight notification message via SQS.
3.  **On-Demand Aggregation**: Only when a job reaches 100% completion does the Manager list the result files from S3 to generate the summary. This ensures that memory usage during the processing phase remains constant (O(1)) per job, regardless of the number of tasks.

### SQS Architecture & Message Flow
The system employs a multi-queue design to effectively separate concerns, ensure scalability, and allow for high-priority signaling.

#### 1. Local App -> Manager (`localApp-to-manager-requests`)
*   **Purpose**: The entry point for all client requests (New Job, Terminate Manager).
*   **Design Choice**: A single, well-known shared queue allows any Local App (Client) to reach the Manager without prior negotiation. It acts as the "Public Interface" of the system.

#### 2. Manager -> Local App (`local-app-output-<UUID>`)
*   **Purpose**: Delivers the final summary link to the specific client that requested it.
*   **Design Choice**: **Dynamic, Per-Job Queues**. Instead of a single shared response queue (which would require every client to peek/filter messages not meant for them), we check a unique queue for each job. This ensures **Response Isolation** and security—clients only receive their own results.

#### 3. Manager -> Workers (`manager-to-workers-tasks`)
*   **Purpose**: The main work distribution channel. Contains URL processing tasks.
*   **Design Choice**: Using a standard SQS queue allows for **Competing Consumers**. We can scale workers up to 100+ instances, and they naturally "compete" for tasks. SQS handles the locking (Visibility Timeout) automatically, preventing processing conflicts.

#### 4. Manager -> Workers Control (`WorkerControlQueue`) ("Poison Pill")
*   **Purpose**: Broadcasts termination signals to workers.
*   **Design Choice**: **Priority Signaling**. If we reused the Task Queue for termination messages, a "Stop" command would sit behind thousands of pending tasks, and workers wouldn't stop until they processed everything. A separate Control Queue allows the "Poison Pill" (Terminate) signal to bypass the backlog and shut down workers immediately.

#### 5. Workers -> Manager Results (`workers-to-manager-results`)
*   **Purpose**: Asynchronous notifications that a task is done (or failed).
*   **Design Choice**: **Asynchronous Aggregation**. Workers don't wait for the Manager to acknowledge. They "fire and forget" the completion status. This decouples the high-speed processing (Worker) from the accounting/tracking (Manager), minimizing the risk of bottlenecks.

## How to Run

### Prerequisites
*   AWS Credentials (`~/.aws/credentials`)
*   Java 11+
*   Maven

### Steps
1.  **Build**:
    ```bash
    mvn clean install
    ```
2.  **Upload JARs**:
    *   `s3://ds-assignment-1-ofek/manager.jar`
    *   `s3://ds-assignment-1-ofek/worker.jar`
3.  **Run Client**:
    ```bash
    java -jar local-app/target/local-app-1.0-SNAPSHOT-shaded.jar input.txt output.html 10
    ```
    *   `n=10`: Tasks per worker ratio.
