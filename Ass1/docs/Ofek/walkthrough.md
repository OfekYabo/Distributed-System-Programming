# Distributed Text Analysis System - Walkthrough

This guide explains how to build, deploy, and run the distributed text analysis system on AWS.

## Prerequisites
- Java 11 installed
- Maven installed
- AWS CLI configured (`aws configure`) with valid credentials
- `deploy.sh` script is executable (run `chmod +x deploy.sh` if needed)

## 1. Build and Deploy
The `deploy.sh` script handles building the Manager and Worker JARs and uploading them to your S3 bucket.

```bash
# Deploy both Manager and Worker
./deploy.sh all
```
> [!NOTE]
> Ensure your `AWS_REGION` and `S3_BUCKET` environment variables are set if you want to override defaults.
> Defaults: Region=`us-east-1`, Bucket=`ds-assignment-1`

## 2. Run Local Application
The Local Application uploads the input file, starts the Manager (if needed), and waits for results.

```bash
# Build Local App (if not already built)
cd local-app
mvn clean package -DskipTests
cd ..

# Run Local App
# Usage: java -jar local-app.jar inputFileName outputFileName n [terminate]
java -jar local-app/target/local-app-1.0-SNAPSHOT.jar input-sample.txt output.html 1 terminate
```

- `input-sample.txt`: Your input file containing URLs and analysis types.
- `output.html`: Where the result summary will be saved.
- `1`: The `n` parameter (files per worker ratio).
- `terminate`: (Optional) Tells the Manager to terminate itself and all workers after completion.

## 3. Monitoring
You can monitor the progress in the terminal. The Local App will log:
- Uploading input file
- Sending task to Manager
- Waiting for response (polling)
- Downloading output file

## 4. Troubleshooting
- **Manager not starting**: Check EC2 console for the Manager instance. Check System Log (Get System Log) to see if user-data script ran successfully.
- **Workers not scaling**: Check `manager.log` on the Manager instance (SSH into it or check CloudWatch if configured).
- **Permissions**: Ensure your AWS credentials have permissions for EC2 (RunInstances, TerminateInstances, etc.), S3 (PutObject, GetObject), and SQS.
