# Assignment 1 Architecture & Planning Log

## Purpose
- Capture the functional requirements from `Assignment 1.txt` together with the reusable LocalApp → Manager → Worker vision described in `PlanningPrompt.txt`.
- Provide a living document we can iterate on while refining abstractions, AWS usage, and implementation details.

## Problem Statement (from `Assignment 1.txt`)
1. Local application uploads an input file with lines shaped as `<analysis>\t<url>` and submits a processing request.
2. Manager fans out tasks per line, orchestrates worker fleet sizing (≤19 instances), monitors progress, aggregates worker outputs, and responds with an HTML summary.
3. Workers pull tasks, download the referenced text file, run Stanford NLP analysis (POS / CONSTITUENCY / DEPENDENCY), upload results to S3, and report completion or failure.
4. Multiple local applications may run concurrently; system must parallelize processing, recover from worker failures, and support termination flow.
5. After completion (or terminate request) the system tears down transient resources (queues, files, instances) to avoid ongoing cost.

## High-Level Objectives
- **Reusable building blocks:** One codebase defining generic AWS communication, queue/message contracts, scaling policies, and lifecycle management that concrete tasks can specialize via configuration/abstract methods.
- **Clear separation of roles:** LocalApp handles submission and cleanup, Manager focuses on orchestration, Workers encapsulate task execution pipelines with pluggable analyzers/handlers.
- **Minimal duplication:** Shared AWS helper layer (S3/SQS/EC2) should be authored once yet remain consumable by binaries deployed to distinct machines.
- **Deterministic resource hygiene:** Every component cleans temporary queues, objects, and instances when safe to do so.

## Component Responsibilities
### Local Application
- Package: stand-alone client running on the student laptop.
- Steps per request:
  1. Upload input file to S3 and register a per-request ID.
  2. Ensure a Manager exists; if not, launch one via EC2 with the proper user-data script.
  3. Create a dedicated `manager→local` response queue named with the request ID.
  4. Send a "new task" message to the shared `local→manager` queue referencing the S3 key, analysis ratios (n), terminate flag, and the callback queue name.
  5. Poll the callback queue until the Manager publishes the summary location (or timeout/abort).
  6. Download the output HTML to the requested local path, delete S3 artifacts, and delete the callback queue.
  7. Optional terminate: send terminate message, wait for ack, verify resources are down.

### Manager (single EC2 instance)
- Continuously consumes from `local→manager` queue using long polling.
- For each local request (identified by RequestId):
  - Download the S3 input file, split into atomic worker tasks, and enqueue messages into `manager→worker` queue. Each worker message contains RequestId, analysis type, source URL, and priority metadata.
  - Track outstanding tasks per RequestId (e.g., JobTracker) with thread-safe structures.
  - Trigger scaling logic: number of required workers = ceil(totalTasks / n) aggregated across active jobs, capped by `ManagerConfig.getMaxWorkerInstances()` and current EC2 state.
  - Listen to `worker→manager` queue, persist partial results (success/error) under a deterministic S3 prefix `results/{RequestId}/...`, and update job progress.
  - Once all tasks of a RequestId finish, build HTML summary (using `HtmlSummaryGenerator`), upload to S3, notify originating LocalApp via its callback queue, and remove in-memory trackers.
  - If terminate message received, stop accepting new jobs, wait for workers to drain, terminate worker fleet, send completions for pending jobs if possible, and shut down.

### Worker (many EC2 instances)
- Loop:
  1. Poll `manager→worker` queue with visibility timeout = processing SLA.
  2. For each message, delegate to a **TaskHandler** object that understands message schema, downloads the text file, applies the requested analysis (Stanford NLP via pluggable strategy), uploads the output to S3, and produces a structured response.
  3. Post the response message to `worker→manager` queue (success or error) including RequestId and any diagnostic reason.
  4. Delete the processed message; handle retries by letting un-acked messages reappear.
- Worker should be multi-threaded if analysis latency allows parallelism (bounded by CPU/RAM).

## AWS Resource Abstractions
- **S3Service**: encapsulate bucket names, upload/download helpers, temporary prefixes, and cleanup utilities (already partially present under `manager/src/.../service`). Extend to be shareable by LocalApp & Worker via an abstracted interface plus parameterized constructors.
- **SqsService**: wrap queue creation/deletion, long polling, batch send/receive, and JSON serialization. Should expose higher-level methods like `sendTaskMessage(TaskMessage)` and `receiveResults(int max)`.
- **Ec2Service**: manage manager/worker instance lifecycle (launch, describe, terminate, waiters) with tagging to distinguish roles. Provide helper for checking existing manager, scaling workers up/down, and waiting for termination.
- Consider extracting these services into a common library module (e.g., `shared-aws`) built once and published (as a jar or classpath dependency) for LocalApp/Manager/Worker to reuse. Alternative is source-level sharing via Git submodule or Maven multi-module project.

## Queue & Message Contracts
| Queue | Producer → Consumer | Payload core fields |
| --- | --- | --- |
| `local→manager` (global) | LocalApp → Manager | RequestId, InputS3Key, OutputQueueName, ratio `n`, terminate flag |
| `manager→worker` (global) | Manager → Worker | RequestId, TaskId, AnalysisType, SourceUrl, optional priority |
| `worker→manager` (global) | Worker → Manager | RequestId, TaskId, status (SUCCESS/ERROR), OutputS3Key or error summary |
| `manager→local` (per request) | Manager → LocalApp | RequestId, SummaryS3Key, status, metrics |

## Scaling & Lifecycle Rules
- Worker target count = min(`maxInstances`, ceil(totalPendingTasks / filesPerWorker)). Workers terminate when backlog shrinks below threshold for a configurable cool-down period (`ManagerConfig.scalingIntervalSeconds`).
- Always tag instances with `Role=manager|worker` and `RequestId` (for debugging) using `Ec2Service` helpers.
- LocalApp deletes only the queues/bucket objects it created; Manager is responsible for worker instance teardown and shared queue hygiene if the entire system terminates.
- Timeouts:
  - SQS visibility timeouts must exceed worst-case worker processing time + buffer to avoid duplicate work (default 5 minutes).
  - Manager idle timeout triggers self-termination if no jobs/workers for `idleTimeoutMinutes`.

## Reusability & Abstraction Strategy
- Define abstract base classes/interfaces:
  - `AbstractAwsClientFactory` for wiring region/credentials once.
  - `BaseTaskMessage`, `BaseTaskHandler`, `BaseJobTracker` encapsulating generics for RequestId/Task payloads.
  - `AbstractManager` providing template methods (`fetchRequests()`, `scheduleWorkers()`, `aggregateResults()`) while delegating domain specifics (e.g., `buildSummary()`).
  - `AbstractWorker` orchestrating poll→handle→respond loop; concrete implementations inject `TaskHandler` strategy.
- Configuration objects (`ManagerConfig`, `WorkerConfig`, `LocalAppConfig`) expose getters; consider YAML/env binding to keep AWS params out of code.

## Current Questions / Decisions to Track
1. **Shared AWS Library Distribution**: prefer single Maven multi-module project producing a `shared-aws` jar consumed by LocalApp/Manager/Worker modules to avoid source duplication while still compiling into separate deployable jars.
2. **Worker Handler Extensibility**: confirm the plan to inject a handler object per task type (Stanford NLP today, other processors later). Need interface definition and serialization format for message payloads.
3. **Monitoring & Metrics**: decide whether to log metrics to CloudWatch or simply persist summary stats in S3 / callback messages.
4. **Termination Semantics**: clarify how Manager distinguishes "terminate after this request" vs "terminate immediately", and whether LocalApp waits for confirmation before cleaning shared queues.

---
_Rev 1 (2025-11-28): Initial structure created. Ready for iterative updates with additional answers/questions as planning progresses._
