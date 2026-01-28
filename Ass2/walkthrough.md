# Walkthrough: Distributed System Programming - Assignment 2 (Collocation Extraction)

## Completed Work

### 1. Execution & Analysis (Full Run)
*   **Version:** v3 (Optimized StringTokenizer)
*   **English Dataset:** Run Initiated (Estimated ~2.5 hours).
*   **Hebrew Dataset:** Identified bug in v2 (Regex `\W` stripped Hebrew) -> Fixed in v3. User to re-run.
*   **Performance:** v3 expected to be 2-3x faster in Step 1.

### 2. Resume Capability (New in v4)
*   **Goal:** Allow resuming a long-running job if the Lab Session (4 hours) expires.
*   **Implementation:** `Main.java` modified to accept `startStep`.
*   **Supported Modes:**
    1.  **Regular Start (Default):** Runs Steps 1 -> 4.
    2.  **Resume from Step 2:** Skips Step 1 (uses existing S3 output), Runs Steps 2 -> 4.
*   **Usage:**
    ```bash
    # Normal
    hadoop jar Ass2.jar s3://.../input s3://.../output

    # Resume from Step 2 (if Step 1 finished)
    hadoop jar Ass2.jar s3://.../input s3://.../output -DstartStep=2
    ```

### 3. Artifacts
*   `full_stats_report.md`: Detailed timing/counter analysis of the v2 run.
*   `hebrew_stats_report.md`: Analysis of the Hebrew data loss.
*   `task.md`: Tasks and progress tracking.

