# Hadoop Validation & Setup Guide for Windows

To run Hadoop MapReduce locally on Windows (even in "local" mode), you need native Windows binaries. Without them, you will see `UnsatisfiedLinkError` or `IOException: Could not locate executable null\bin\winutils.exe`.

## 1. Download Native Binaries
The official Apache Hadoop distribution does not include Windows binaries. You must use a community-maintained version.

1.  Go to the **cdarlint/winutils** GitHub repository: [https://github.com/cdarlint/winutils](https://github.com/cdarlint/winutils)
2.  Navigate to the folder matching your Hadoop version.
    *   Since your `pom.xml` uses `3.3.6`, look for `hadoop-3.3.6`.
    *   If `3.3.6` is missing, **`hadoop-3.3.5`** is fully compatible and recommended.
3.  Download the entire `bin` folder from that version.
    *   *Easier method:* Download the whole repo as a ZIP, unzip it, and find the `hadoop-3.3.5/bin` folder.

## 2. Install
1.  Create a folder on your computer, e.g., `C:\hadoop`.
2.  Copy the `bin` folder you downloaded into it.
    *   You should end up with: `C:\hadoop\bin\winutils.exe`, `C:\hadoop\bin\hadoop.dll`, etc.

## 3. Configure Environment Variables
Windows needs to know where these files are.

1.  Open **System Properties** -> **Advanced** -> **Environment Variables**.
2.  **Add User Variable**:
    *   Variable Name: `HADOOP_HOME`
    *   Variable Value: `C:\hadoop`
3.  **Edit Path Variable**:
    *   Find the `Path` variable (User or System).
    *   Click **Edit** -> **New**.
    *   Add: `%HADOOP_HOME%\bin` (or `C:\hadoop\bin`).

## 4. Verification
1.  **Restart** your terminal or VS Code (important!).
2.  Open a terminal and type:
    ```cmd
    winutils
    ```
    *   If you see usage help, it works!
    *   If you see "not recognized", check your PATH.

## 5. Run Your Project
You can now run your project with:
```bash
java -jar target/Ass2-1.0-SNAPSHOT-shaded.jar C:/input C:/output eng regular true local
```
(Ensure you use the `local` argument at the end).
