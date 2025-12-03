# Implementation Plan - .env Configuration Refactor

## Goal
Refactor `LocalApp`, `Manager`, and `Worker` to load configuration from an `.env` file instead of using hardcoded default values. This improves security and configurability.

## User Review Required
- **Dependency**: Adding `io.github.cdimascio:java-dotenv` to `pom.xml` files.
  > **Explanation**: This library is required to read `.env` files. Java does not have built-in support for reading `.env` files, so we use this library to parse the file and load the variables into the system environment.
- **Behavior Change**: Missing environment variables will now cause the application to fail fast.
- **Configuration Structure**: Configuration will be split into 4 files: `aws.env` (shared), `local.env`, `manager.env`, and `worker.env`.

## Proposed Changes

### 1. Environment Files
We will create 4 separate configuration files to allow for modularity and reuse.

#### [NEW] [aws.env](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/aws.env)
Shared AWS configuration (Region, Bucket, Queue Names).
```properties
# AWS Region & S3
AWS_REGION=us-east-1
S3_BUCKET_NAME=ds-assignment-1

# Queue Names (Shared Interface)
LOCAL_APP_INPUT_QUEUE=local-app-requests-queue
LOCAL_APP_OUTPUT_QUEUE=local-app-responses-queue
WORKER_INPUT_QUEUE=worker-tasks-queue
WORKER_OUTPUT_QUEUE=worker-results-queue
```

#### [NEW] [local.env](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/local.env)
Configuration specific to the Local Application.
```properties
# Manager Launch Configuration
MANAGER_AMI_ID=ami-0fa3fe0fa7920f68e
MANAGER_INSTANCE_TYPE=t2.micro
MANAGER_IAM_ROLE=ass1-manager
MANAGER_SECURITY_GROUP=sg-097d5427acd34969e
MANAGER_KEY_NAME=ds
```

#### [NEW] [manager.env](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/manager.env)
Configuration for the Manager logic and Worker launching.
```properties
# Worker Launch Configuration
WORKER_AMI_ID=ami-0fa3fe0fa7920f68e
WORKER_INSTANCE_TYPE=t2.micro
WORKER_IAM_ROLE=ass1-worker
WORKER_SECURITY_GROUP=
WORKER_KEY_NAME=
MAX_WORKER_INSTANCES=19

# Manager Behavior
IDLE_TIMEOUT_MINUTES=5
SCALING_INTERVAL_SECONDS=30
```

#### [NEW] [worker.env](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/worker.env)
Configuration for the Worker logic.
```properties
# Worker Behavior
MAX_PROCESSING_TIME_SECONDS=240
TEMP_DIR=/tmp/worker
```

### 2. Configuration Propagation to EC2
To pass these configurations to the remote EC2 instances, we will use **User Data** scripts.

#### LocalApp -> Manager
When `LocalApp` launches the Manager EC2 instance:
1.  `LocalApp` reads `aws.env` and `manager.env` locally.
2.  It constructs a **User Data** script (bash script) for the Manager instance.
3.  This script will programmatically create a `.env` file on the Manager instance containing the values read from `aws.env` and `manager.env`.
4.  The Manager application starts and reads this local `.env` file.

#### Manager -> Worker
When `Manager` launches Worker EC2 instances:
1.  `Manager` reads `aws.env` and `worker.env` (which were passed to it or available to it).
2.  It constructs a **User Data** script for the Worker instance.
3.  This script creates a `.env` file on the Worker instance with values from `aws.env` and `worker.env`.
4.  The Worker application starts and reads this local `.env` file.

**Note**: This ensures that if you change `worker.env` locally, the Manager (if restarted/redeployed) will pick up the new values and pass them to new Workers.

### 2. Dependencies
Add `java-dotenv` to `local-app/pom.xml`, `manager/pom.xml`, and `worker/pom.xml`.

```xml
<dependency>
<groupId>io.github.cdimascio</groupId>
<artifactId>java-dotenv</artifactId>
<version>5.2.2</version>
</dependency>
```

### 3. Code Refactoring

#### [MODIFY] [LocalAppConfig.java](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/local-app/src/main/java/com/distributed/systems/LocalAppConfig.java)
- Initialize `Dotenv` loading both `aws.env` and `local.env`.
- Example: `Dotenv.configure().filename("aws.env").load();`
- Replace `getEnvOrDefault` with direct `dotenv.get()` calls.
- Throw `RuntimeException` if a required variable is missing.

#### [MODIFY] [ManagerConfig.java](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/manager/src/main/java/com/distributed/systems/ManagerConfig.java)
- Initialize `Dotenv` loading `aws.env` and `manager.env`.
- Handle `WORKER_SECURITY_GROUP` and `WORKER_KEY_NAME` as optional.

#### [MODIFY] [WorkerConfig.java](file:///c:/Users/ofeky_xoerp0y/Vscode_GitRepository/School/Distributed-System-Programming/Ass1/worker/src/main/java/com/distributed/systems/WorkerConfig.java)
- Initialize `Dotenv` loading `aws.env` and `worker.env`.

## Verification Plan

### Automated Tests
- Run `mvn test` in each module.

### Manual Verification
1.  **Local Execution**:
- Create `.env` with defaults.
- Run `LocalApp` (or a test main) and verify it starts.
- Rename `.env` and verify it fails.
