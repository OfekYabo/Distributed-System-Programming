# Shared AWS Service Layer Implementation Plan

## Goal
Refactor the existing `local-app`, `manager`, and `worker` applications to use a shared, generalized AWS service layer. This will reduce code duplication, ensure consistent error handling, and simplify future maintenance. The project will be restructured into a multi-module Maven project.

## User Review Required
> [!IMPORTANT]
> **Project Structure Change**: The project will be converted to a multi-module Maven project with a root `pom.xml`. This is a significant structural change.
> **Dependency**: `local-app`, `manager`, and `worker` will depend on the new `shared-lib` module.

## Proposed Changes

### 1. Project Structure
Create a root `pom.xml` to manage the modules:
- `shared-lib` (New)
- `local-app` (Existing)
- `manager` (Existing)
- `worker` (Existing)

### 2. Shared Library (`shared-lib`)
Create a new module `shared-lib` with the following components:

#### [NEW] `pom.xml`
- Dependencies: AWS SDK (S3, SQS, EC2), SLF4J, Jackson.

#### [NEW] `com.distributed.systems.shared.AwsClientFactory`
- Factory to create `S3Client`, `SqsClient`, `Ec2Client`.
- Handles region configuration and credentials (default chain).

#### [NEW] `com.distributed.systems.shared.service.S3Service`
- Generalized S3 operations (upload, download, string/file handling).
- Merges functionality from `LocalApp` and `Manager` (including presigned URLs).
- Decoupled from specific Config classes (accepts `S3Client` and bucket name).

#### [NEW] `com.distributed.systems.shared.service.SqsService`
- Generalized SQS operations (send, receive, delete, queue creation).
- Decoupled from specific Config classes.

#### [NEW] `com.distributed.systems.shared.service.Ec2Service`
- Generalized EC2 operations (start instance, check status, terminate).

#### [NEW] `com.distributed.systems.shared.model`
- Shared message models (e.g., `LocalAppRequest`, `LocalAppResponse`, `WorkerTaskMessage`) if they are shared. (Currently they seem copied or distinct, will investigate if they can be unified).

### 3. Refactoring Existing Modules

#### `local-app`
- **pom.xml**: Add dependency on `shared-lib`.
- **LocalAppConfig**: Keep as is, but maybe implement a common interface if useful.
- **LocalApp.java**:
    - Use `AwsClientFactory` to create clients.
    - Use `shared.service.S3Service`, `shared.service.SqsService`, `shared.service.Ec2Service`.
    - Remove local `service` package.

#### `manager`
- **pom.xml**: Add dependency on `shared-lib`.
- **Manager.java**:
    - Use `AwsClientFactory` and shared services.
    - Remove local `service` package.

#### `worker`
- **pom.xml**: Add dependency on `shared-lib`.
- **Worker.java**:
    - Use `AwsClientFactory` from shared lib.
    - Refactor `SqsMessageHandler` and `S3Uploader` to use or extend shared services if applicable, or keep them if they are too specific but use the shared clients.

## Verification Plan

### Automated Tests
- **Unit Tests**: Add unit tests in `shared-lib` for the services (mocking AWS clients).
- **Build Verification**: Run `mvn clean install` from root to ensure all modules build and tests pass.

### Manual Verification
- **End-to-End Test**:
    1.  Build the project: `mvn clean install`.
    2.  Run `local-app` with a sample input file.
    3.  Verify it launches `manager`.
    4.  Verify `manager` launches `worker`.
    5.  Verify processing completes and output is downloaded.
    6.  Verify termination works.
