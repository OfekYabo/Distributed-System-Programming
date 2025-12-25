# Implementation Plan - .env Configuration Refactor

## Goal
Refactor `LocalApp`, `Manager`, and `Worker` to load configuration from an `.env` file instead of using hardcoded default values. This improves security and configurability.

## User Review Required
- **Dependency**: Adding `io.github.cdimascio:java-dotenv` to `pom.xml` files.
  > **Explanation**: This library is required to read `.env` files. Java does not have built-in support for reading `.env` files, so we use this library to parse the file and load the variables into the system environment.
- **Behavior Change**: Missing environment variables will now cause the application to fail fast.
- **Configuration Structure**: We will use a single `.env` file with clear section comments to separate configuration for different components.

## Proposed Changes

### 1. Environment Files

#### [NEW] [.env.example](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/.env.example)
Contains all configuration keys with empty values/comments.
```properties
# ==========================================
# AWS Configuration (Shared)
# ==========================================
AWS_REGION=
S3_BUCKET_NAME=

# ==========================================
# Queue Configuration (Shared)
# ==========================================
LOCAL_APP_INPUT_QUEUE=
LOCAL_APP_OUTPUT_QUEUE=
WORKER_INPUT_QUEUE=
WORKER_OUTPUT_QUEUE=

# ==========================================
# EC2 Configuration (LocalApp -> Manager)
# ==========================================
MANAGER_AMI_ID=
MANAGER_INSTANCE_TYPE=
MANAGER_IAM_ROLE=
MANAGER_SECURITY_GROUP=
MANAGER_KEY_NAME=

# ==========================================
# EC2 Configuration (Manager -> Worker)
# ==========================================
WORKER_AMI_ID=
WORKER_INSTANCE_TYPE=
WORKER_IAM_ROLE=
WORKER_SECURITY_GROUP=
WORKER_KEY_NAME=
MAX_WORKER_INSTANCES=

# ==========================================
# SQS Configuration (Shared)
# ==========================================
VISIBILITY_TIMEOUT_SECONDS=
WAIT_TIME_SECONDS=
MAX_MESSAGES=

# ==========================================
# Local App Behavior
# ==========================================
POLL_INTERVAL_SECONDS=

# ==========================================
# Manager Behavior
# ==========================================
IDLE_TIMEOUT_MINUTES=
SCALING_INTERVAL_SECONDS=

# ==========================================
# Worker Behavior
# ==========================================
MAX_PROCESSING_TIME_SECONDS=
TEMP_DIR=
WORKER_MAX_MESSAGES=
```

#### [NEW] [.env](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/.env)
Contains the current default values.
```properties
# ==========================================
# AWS Configuration (Shared)
# ==========================================
AWS_REGION=us-east-1
S3_BUCKET_NAME=ds-assignment-1

# ==========================================
# Queue Configuration (Shared)
# ==========================================
LOCAL_APP_INPUT_QUEUE=local-app-requests-queue
LOCAL_APP_OUTPUT_QUEUE=local-app-responses-queue
WORKER_INPUT_QUEUE=worker-tasks-queue
WORKER_OUTPUT_QUEUE=worker-results-queue

# ==========================================
# EC2 Configuration (LocalApp -> Manager)
# ==========================================
MANAGER_AMI_ID=ami-0fa3fe0fa7920f68e
MANAGER_INSTANCE_TYPE=t2.micro
MANAGER_IAM_ROLE=ass1-manager
MANAGER_SECURITY_GROUP=sg-097d5427acd34969e
MANAGER_KEY_NAME=ds

# ==========================================
# EC2 Configuration (Manager -> Worker)
# ==========================================
WORKER_AMI_ID=ami-0fa3fe0fa7920f68e
WORKER_INSTANCE_TYPE=t2.micro
WORKER_IAM_ROLE=ass1-worker
WORKER_SECURITY_GROUP=
WORKER_KEY_NAME=
MAX_WORKER_INSTANCES=19

# ==========================================
# SQS Configuration (Shared)
# ==========================================
VISIBILITY_TIMEOUT_SECONDS=300
WAIT_TIME_SECONDS=20
MAX_MESSAGES=10

# ==========================================
# Local App Behavior
# ==========================================
POLL_INTERVAL_SECONDS=5

# ==========================================
# Manager Behavior
# ==========================================
IDLE_TIMEOUT_MINUTES=5
SCALING_INTERVAL_SECONDS=30

# ==========================================
# Worker Behavior
# ==========================================
MAX_PROCESSING_TIME_SECONDS=240
TEMP_DIR=/tmp/worker
WORKER_MAX_MESSAGES=1
```

### 2. Configuration Propagation to EC2
To pass these configurations to the remote EC2 instances, we will use **User Data** scripts.

#### LocalApp -> Manager
When `LocalApp` launches the Manager EC2 instance:
1.  `LocalApp` reads the local `.env` file.
2.  It constructs a **User Data** script (bash script) for the Manager instance.
3.  This script will programmatically create a `.env` file on the Manager instance containing the **entire content** of the local `.env` file.
4.  The Manager application starts and reads this local `.env` file.

#### Manager -> Worker
When `Manager` launches Worker EC2 instances:
1.  `Manager` reads its own `.env` file (which it received from LocalApp).
2.  It constructs a **User Data** script for the Worker instance.
3.  This script creates a `.env` file on the Worker instance, again propagating the configuration.
4.  The Worker application starts and reads this local `.env` file.

### 3. Dependencies
Add `java-dotenv` to `local-app/pom.xml`, `manager/pom.xml`, and `worker/pom.xml`.

```xml
<dependency>
    <groupId>io.github.cdimascio</groupId>
    <artifactId>java-dotenv</artifactId>
    <version>5.2.2</version>
</dependency>
```

### 4. Code Refactoring

#### [MODIFY] [LocalAppConfig.java](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/local-app/src/main/java/com/distributed/systems/LocalAppConfig.java)
- Initialize `Dotenv` in constructor: `Dotenv dotenv = Dotenv.load();`
- Replace `getEnvOrDefault` with direct `dotenv.get()` calls.
- Throw `RuntimeException` if a required variable is missing.

#### [MODIFY] [ManagerConfig.java](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/manager/src/main/java/com/distributed/systems/ManagerConfig.java)
- Initialize `Dotenv` in constructor.
- Handle `WORKER_SECURITY_GROUP` and `WORKER_KEY_NAME` as optional.

#### [MODIFY] [WorkerConfig.java](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/worker/src/main/java/com/distributed/systems/WorkerConfig.java)
- Initialize `Dotenv` in constructor.
- **Change**: Read `WORKER_MAX_MESSAGES` instead of `MAX_MESSAGES` to distinguish from the shared SQS limit.

## Verification Plan

### Automated Tests
- Run `mvn test` in each module.

### Manual Verification
1.  **Local Execution**:
    - Create `.env` with defaults.
    - Run `LocalApp` (or a test main) and verify it starts.
    - Rename `.env` and verify it fails.
