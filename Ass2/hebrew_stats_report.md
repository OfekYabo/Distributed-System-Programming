
# Hadoop MapReduce Execution Report - Hebrew (Full Run / v2)

## Overview
This report analyzes the execution of the Hebrew Full Run (v2).
**Status:** **WARNING: EXTREME DATA LOSS DETECTED**

---

## Job 1 (Step 1): Initial N-Gram Count
*   **Job ID:** `job_..._0005`
*   **Wall-Clock Duration:** **~4 Minutes** (21:45:26 - 21:49:14)
*   **Inputs:** `s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data`

### Statistics
| Metric | Value | Note |
| :--- | :--- | :--- |
| **Map Input Records** | `252,069,581` | ~252 Million records read. |
| **Map Output Records** | `585` | **CRITICAL: Only 585 records emitted!** |
| **Drop Rate** | **~99.9997%** | Almost 100% of data was filtered out. |
| **Combiner Output** | `137` | |
| **Reduce Output Records** | `137` | |

### Analysis
*   The system read 252 million records but discarded almost 584,999,000 of them.
*   This indicates that nearly **every single line** failed one of the validation checks in the Mapper (`AggregationMapper`).

---

## Job 2 (Step 2): Total Count (N) Calculation
*   **Job ID:** `job_..._0006`
*   **Wall-Clock Duration:** **~1 Minute** (21:49:34 - 21:50:14)

### Statistics
| Metric | Value |
| :--- | :--- |
| **Map Input Records** | `137` |
| **Map Output Records** | `274` |
| **Reduce Output Records** | `137` |
| **Decade Counts (N)** | Very low (e.g., `N_1950=147`, `N_2000=510`). |

---

## Job 3 (Step 3): Join / PMI Score Calculation
*   **Job ID:** `job_..._0007`
*   **Wall-Clock Duration:** **~1 Minute**

### Statistics
| Metric | Value |
| :--- | :--- |
| **Map Input Records** | `137` |
| **Map Output Records** | `274` |
| **Reduce Output Records** | `137` |

---

## Job 4 (Step 4): Sorting
*   **Job ID:** `job_..._0008`
*   **Wall-Clock Duration:** **~1 Minute**

### Statistics
| Metric | Value |
| :--- | :--- |
| **Input** | `137` records |
| **Output** | `137` records |

---

## Diagnosis & Potential Causes
Why did Step 1 filter out 99.9997% of Hebrew data?

1.  **Stop Words:**
    *   If `heb-stopwords.txt` works, it shouldn't filter *everything*. (Unlikely to be the sole cause).
2.  **Sanitization Logic (Regex):**
    *   The v2 code used: `if (!cleaned.matches(".*[a-zA-Z\u0590-\u05FF].*"))`
    *   This logic *should* pass Hebrew characters.
3.  **Input Format Mismatch (Most Likely):**
    *   We are using `SequenceFileInputFormat`.
    *   If the Hebrew dataset on S3 is actually **Text** (not SequenceFile), reading it as SequenceFile results in binary garbage.
    *   If the Mapper reads garbage, `parts.length < 4` check likely fails immediately, causing the record to be skipped.
4.  **Parsing/Splitting:**
    *   The `split("\\s+")` might rely on specific encoding. If the input is not correctly interpreted as UTF-8, the spaces might not match `\\s`.

### Recommendation
*   **Verify Input Format:** Check if the Hebrew S3 path contains SequenceFiles or TextFiles.
*   **Check V3:** The new logic (v3) uses manual char checking. If the issue is complex regex behavior on the EMR JVM, v3 might fix it. BUT if the issue is Input Format, v3 will still fail.
