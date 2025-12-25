# Distributed Text Analysis System
**Authors:** 

Ofek Yabo, ID: 209288588

Amit Zarhi, ID: 208230235


**Course:** Distributed Systems Programming - Assignment 1

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Concurrency & Threading Model](#concurrency--threading-model)
    - [1. Manager (4 Threads)](#1-manager-4-threads)
    - [2. Worker (2 Threads)](#2-worker-2-threads)
    - [3. Local App (1 Thread)](#3-local-app-1-thread)
- [Workflow Dynamics](#workflow-dynamics)
- [Configuration (.env)](#configuration-env)
    - [AWS & Resources](#aws--resources)
    - [Queues (SQS)](#queues-sqs)
    - [Performance](#performance)
    - [Worker Logic](#worker-logic)
- [Design Considerations](#design-considerations)
    - [Memory Efficiency & Scalability](#memory-efficiency--scalability)
    - [Why SQS?](#why-sqs)
    - [SQS Architecture & Message Flow](#sqs-architecture--message-flow)
- [Critical Analysis & Scalability](#critical-analysis--scalability)
    - [1. Scalability](#1-scalability-1-million-to-1-billion-clients)
    - [2. Persistence & Fault Tolerance](#2-persistence--fault-tolerance)
    - [3. Threading Rationale](#3-threading-rationale)
    - [4. Worker Utilization](#4-worker-utilization)
    - [5. Distributed Nature](#5-distributed-nature)
- [Performance Statistics](#performance-statistics-sample-run)
- [How to Run](#how-to-run)

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
    -   **Aggregator**: Collects results from Workers, monitors jobs progress, updates the needed workers count and generates a final HTML summary to S3.
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
*   **Main Thread**: Lifecycle manager. Polls for a task, submits it to the executor, and waits for completion (monitoring for timeouts).
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

### Worker Logic
*   `WORKER_MAX_MESSAGES`: `1` (Process one task at a time).
*   `WORKER_MAX_SENTENCE_LENGTH`: `20` (Skips sentences longer than 20 words during Constituency Parsing to prevent timeouts).
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

## Critical Analysis & Scalability

### 1. Scalability (1 Million to 1 Billion Clients)
*   **Horizontal Scaling**: The system is designed to scale horizontally. Since the Manager and Workers are decoupled via SQS, we can theoretically add thousands of Worker instances.
*   **Bottlenecks**:
    *   **Manager**: The Manager is currently a Single Point of Failure (SPOF) and a potential bottleneck. For 1 billion clients, a single EC2 instance cannot handle the thread management and aggregation.
    *   **Solution**: To scale to billions, we would need to shard the Manager (e.g., one Manager per region or per user group).

### 2. Persistence & Fault Tolerance
*   **Node Death**: If a Worker node dies (power failure, crash), the SQS **Visibility Timeout** ensures the message reappears in the queue after 1000s. Another worker will pick it up. The task is never lost.
*   **Decoupled State**: The state is stored in SQS (tasks) and S3 (results). The Manager is mostly stateless regarding task progress (it recovers state by checking SQS/S3, though strictly speaking, our current in-memory `JobTracker` would lose counters on crash—a trade-off for simplicity).

### 3. Threading Rationale
*   **Why Threads?**: We use threads in the Manager (`LocalAppListener`, `WorkerResultsListener`) because we are **I/O bound**. Most time is spent waiting for network responses (SQS/S3). Blocking the main thread for these operations would freeze the system.
*   **Why Not More?**: We limited threads to a fixed pool to prevent context-switching overhead.
*   **Workers**: Use a single processing thread + a monitoring thread. This simplifies logic; since each worker runs on a dedicated core/instance, adding more threads to a single worker for CPU-intensive NLP (Constituency Parsing) would just cause contention.

### 4. Worker Utilization
*   **"Working Hard?"**: Yes. The **Competing Consumers** pattern ensures that as soon as a worker finishes, it grabs the next task. No worker sits idle if there is work to do.
*   **Load Balancing**: SQS handles this natively.

### 5. Distributed Nature
*   **Nothing Waits**: The Local App sends a request and *polls*. The Manager sends tasks and *continues*. Workers process and *upload*. No component statically waits for another component to be "online" in a synchronized socket connection. This is a truly asynchronous distributed system.

## Performance Statistics (Sample Run)
*   **Instance Types Used**:
    *   Manager: `t2.micro`
    *   Workers: `t3.medium`
*   **Input File**: `input-sample.txt` (the given example file)
*   **n (Workers Ratio)**: `1`
*   **Total Execution Time**: 
    *   **Cold Start** (Launching Manager + Workers): ~345 seconds (~5.7 mins)
    *   **Warm Start** (Launching Workers, Manager already running): ~290 seconds (~4.8 mins)

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
    java -jar local-app/target/local-app-1.0-SNAPSHOT.jar input.txt output.html 1
    ```
    *   `n=1`: Tasks per worker ratio.
