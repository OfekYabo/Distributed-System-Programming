# Project Overview and Checklist

## The Task in Short
The goal is to build a distributed system on AWS to analyze text files.
1.  **Input**: A user provides a file with a list of URLs (text files) and the type of analysis to perform (POS, Constituency, Dependency).
2.  **Process**:
    *   **Local App**: Uploads input to S3, starts a Manager on EC2.
    *   **Manager**: Downloads the list, distributes tasks to Workers (scaling them based on load), and aggregates results.
    *   **Workers**: Download text files, perform NLP analysis (using Stanford NLP), and upload results to S3.
3.  **Output**: An HTML file summarizing the results (Input URL -> Output S3 URL).

## Checklist
Legend:
- [v] Implemented
- [x] Implemented but needs fix/review
- [ ] Not implemented

### Local Application
- [v] Parse arguments (`input`, `output`, `n`, `terminate`)
- [v] Check if Manager is active; start if not (EC2)
- [v] Upload input file to S3
- [v] Send "New Task" message to Manager (SQS)
- [v] Poll for "Done" message from Manager (SQS)
- [v] Download summary HTML from S3
- [v] Handle `terminate` argument (send termination message)

### Manager
- [v] Listen for messages from Local App
- [v] Download input file from S3
- [v] Create SQS messages for each URL/Task
- [v] **Auto-scaling**: Launch 1 worker per `n` messages (up to 19 max)
- [v] Listen for "Task Done" messages from Workers
- [v] Aggregate results (track which job is done)
- [v] Generate summary HTML and upload to S3
- [v] Notify Local App when done
- [v] Handle termination (stop accepting jobs, wait for workers, terminate instances)

### Worker
- [v] Poll for "Analysis Task" messages
- [v] Download text file from URL
- [v] Perform NLP Analysis (POS, Constituency, Dependency)
- [v] Upload result to S3
- [v] Send "Task Done" message to Manager
- [v] Handle errors (catch exception, send error message)

### General / Infrastructure
- [v] S3 Bucket creation/usage
- [v] SQS Queue creation/usage
- [v] EC2 Instance launching (AMI, User Data script)
- [v] Security (IAM Roles, Security Groups - assumed configured in AWS)
- [ ] **Full System Test on AWS** (Requires deployment)
