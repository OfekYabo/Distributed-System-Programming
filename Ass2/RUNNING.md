# MapReduce Assignment 2 - Run Guide

This guide explains how to compile the project, run it locally on Windows (using a small dataset), and deploy it to AWS EMR (for the full dataset).

## Prerequisites

1.  **Java 8**: Ensure JDK 8 is installed and `JAVA_HOME` is set.
2.  **Maven**: For building the project.
3.  **Hadoop (Local)**: `HADOOP_HOME` and `bin/winutils.exe` must be set up correctly on Windows.
4.  **AWS CLI** (Optional): For uploading files to S3.

## 1. Build the Project

Open a terminal in the project root (`Ass2` folder) and run:

```powershell
mvn clean package
```

This will create `Ass2-1.0-SNAPSHOT.jar` in the `target/` directory.

---

## 2. Run Locally (Windows)

Since the Mapper expects a **SequenceFile** (key: LongWritable, value: Text), we cannot run directly on a plain text file. We must first convert your sample text data into a SequenceFile.

### Step A: Prepare Sample Data
Create a file named `sample.txt` in the root directory with some test data (hebrew/english).

### Step B: Run the Helper Script
I have provided a script `run_local.bat` to automate the conversion and execution execution.

```powershell
.\run_local.bat sample.txt output_local
```

**What this script does:**
1.  Compiles the code (skips if already built).
2.  Runs `com.dsp.ass2.utils.CreateSequenceFile` to convert `sample.txt` -> `sample.seq`.
3.  Runs `com.dsp.ass2.Main` using `sample.seq` as input and `output_local` as output.

### Manual Local Execution
If you want to run manually without the script:
```powershell
# 1. Convert Text to SequenceFile
java -cp "target/Ass2-1.0-SNAPSHOT.jar;%HADOOP_HOME%/share/hadoop/common/*;%HADOOP_HOME%/share/hadoop/mapreduce/*" com.dsp.ass2.utils.CreateSequenceFile sample.txt sample.seq

# 2. Run MapReduce Job
hadoop jar target/Ass2-1.0-SNAPSHOT.jar com.dsp.ass2.Main sample.seq output_local
```

---

## 3. Run on AWS EMR

### Step A: Upload Files to S3
1.  **Create a Bucket**: `s3://ds-assignment-2-ofek/` (Region: `us-east-1` recommended).
2.  **Upload JAR**: Upload `target/Ass2-1.0-SNAPSHOT.jar` to the bucket.
3.  **Data**: Ensure you have the dataset path (e.g., Google N-Grams).

### Step B: Configure EMR
1.  Go to **EMR Console** in AWS.
2.  **Create Cluster**:
    *   **Release**: `emr-5.36.0` (or similar 5.x).
    *   **Applications**: Hadoop.
    *   **Instance Type**: `m4.large` or similar (available in AWS Academy labs).
    *   **Cluster Name**: `DSP-Ass2-Cluster`.
    *   **Log URI**: `s3://ds-assignment-2-ofek/logs/` (Optional but recommended).

### Step C: Add Step (Run the Job)
Once the cluster is "Waiting", click **Steps** -> **Add step**:
*   **Type**: Custom JAR
*   **Name**: `Run Ass2`
*   **JAR location**: `s3://ds-assignment-2-ofek/Ass2-1.0-SNAPSHOT.jar`
*   **Action on failure**: Continue (so you can debug logs).

## Predefined EMR Runs
Copy and paste the relevant arguments below into the **Arguments** field of the EMR "Add Step" dialog.

### 1. 2 Decades (1990-2000) - English
```text
-DstartDecade=1990 -DendDecade=2000 com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data s3://ds-assignment-2-ofek/output/eng_2_decades
```

### 2. 2 Decades (1990-2000) - Hebrew
```text
-DstartDecade=1990 -DendDecade=2000 com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data s3://ds-assignment-2-ofek/output/heb_2_decades
```

### 3. 10 Decades (1900-1990) - English
```text
-DstartDecade=1900 -DendDecade=1990 com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data s3://ds-assignment-2-ofek/output/eng_10_decades
```

### 4. 10 Decades (1900-1990) - Hebrew
```text
-DstartDecade=1900 -DendDecade=1990 com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data s3://ds-assignment-2-ofek/output/heb_10_decades
```

### 5. Full Run - English
```text
com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data s3://ds-assignment-2-ofek/output/eng_full
```

### 6. Full Run - Hebrew
```text
com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data s3://ds-assignment-2-ofek/output/heb_full
```

### Step D: Monitor & Download Results
1.  Monitor the step status in the EMR console.
2.  Check `stdout` / `stderr` logs for "Counters" output.
3.  Once "Completed", go to S3 to see the results.

### 7. Verification: Local Sample on EMR
First, upload your local sequence file (e.g., `data/input/googlebooks-eng-all-2gram-20090715-0.seq`) to:
`s3://ds-assignment-2-ofek/input/googlebooks-eng-all-2gram-20090715-0.seq`

Then run this step (It will finish in seconds):

```text
com.dsp.ass2.Main s3://ds-assignment-2-ofek/input/googlebooks-eng-all-2gram-20090715-0.seq s3://ds-assignment-2-ofek/output/test_local_file_on_cloud
```

