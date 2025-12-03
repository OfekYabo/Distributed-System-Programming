# Implementation Details

## Big Picture & Architecture
The system follows a **Master-Worker** pattern using AWS services for communication and storage.

### Components
1.  **Local Application** (`local-app`): The client. It triggers the process.
    *   *Connection*: Talks to Manager via SQS (`local-app-requests-queue`). Uses S3 for file transfer.
2.  **Manager** (`manager`): The orchestrator. Runs on EC2.
    *   *Connection*:
        *   Talks to Local App via SQS (`local-app-responses-queue`).
        *   Talks to Workers via SQS (`worker-tasks-queue` and `worker-results-queue`).
        *   Controls EC2 (launches/terminates workers).
3.  **Worker** (`worker`): The processor. Runs on EC2.
    *   *Connection*: Receives tasks from Manager via SQS. Uploads results to S3.

## AWS Services Usage
*   **S3 (Simple Storage Service)**:
    *   Stores input files (uploaded by Local App).
    *   Stores analyzed text files (uploaded by Workers).
    *   Stores the final summary HTML (uploaded by Manager).
    *   Stores the JAR files (`manager.jar`, `worker.jar`) for bootstrapping.
*   **SQS (Simple Queue Service)**:
    *   Decouples components.
    *   **Queues**:
        *   `local-app-input`: Local App -> Manager (New Job, Terminate).
        *   `local-app-output`: Manager -> Local App (Job Done).
        *   `worker-input`: Manager -> Worker (Analysis Task).
        *   `worker-output`: Worker -> Manager (Task Result/Error).
*   **EC2 (Elastic Compute Cloud)**:
    *   Hosts the Manager and Worker instances.
    *   **User Data Scripts**: Used to bootstrap instances (install Java, download JAR from S3, run app).

## Implementation Highlights

### 1. Local Application (`LocalApp.java`)
*   **Manager Check**: Uses `Ec2Service` to check for an instance with tag `Role=manager`. If none, it launches one using a predefined AMI and a User Data script that downloads `manager.jar` from S3.
*   **Polling**: It waits for the Manager to finish by polling the `local-app-output` queue. It filters messages by `jobId` so multiple local apps don't interfere.

### 2. Manager (`Manager.java`)
*   **Multi-threading**:
    *   `LocalAppListener`: Listens for new jobs.
    *   `WorkerResultsListener`: Listens for completed tasks from workers.
    *   `WorkerScaler`: Periodically checks queue depth and launches workers.
*   **Job Tracking (`JobTracker.java`)**:
    *   Keeps track of every task (URL + Operation).
    *   Knows when a job is 100% complete (all tasks returned).
*   **Scaling Logic (`WorkerScaler.java`)**:
    *   Formula: `Required Workers = ceil(Pending Messages / n)`.
    *   It checks every few seconds and launches more workers if needed (up to a max of 19).

### 3. Worker (`Worker.java`)
*   **Task Processing (`TaskProcessor.java`)**:
    *   Downloads the text file from the given URL.
    *   Uses **Stanford NLP** library to perform the requested analysis.
    *   **Lazy Loading**: NLP models are large, so they are loaded only when needed and reused (static fields).
*   **Error Handling**:
    *   If a URL is bad or parsing fails, it catches the exception and sends a "Error" message to the Manager, so the Manager knows not to wait forever.

## Important Files
*   `deploy.sh`: Helper script to build JARs and upload them to S3.
*   `pom.xml`: Dependency management (AWS SDK, Stanford NLP, Jackson).
