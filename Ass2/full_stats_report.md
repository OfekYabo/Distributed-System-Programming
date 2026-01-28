
# Hadoop MapReduce Execution Report

## Overview
This report analyzes the execution of the MapReduce job chain (English 10 Decades, 1990-2000), which consisted of **4 sequential Hadoop jobs** (Job IDs 0009, 0010, 0011, 0012).

---

## Job 1 (Step 1): Initial N-Gram Count
*   **Job ID:** `job_..._0009`
*   **Wall-Clock Duration:** **~1.5 - 2 Hours** (Estimated based on 94%-100% taking 9 minutes)
*   **Goal:** distinct 2-gram counts per decade.
*   **Input:** Google Books N-Grams (Raw).
*   **Output:** `(w1 w2 decade) -> count`

### Statistics
| Metric | Value |
| :--- | :--- |
| **Map Input Records** | `3,923,370,881` |
| **Map Output Records** | `1,139,756,121` |
| **Map Output Size** | `~31.7 GB` (`34,063,047,090` bytes) |
| **Combiner Input** | `1,139,756,121` |
| **Combiner Output** | `240,480,520` (**79% Reduction**) |
| **Reduce Input Records** | `240,480,520` |
| **Reduce Shuffle Size** | `~3.7 GB` (`3,992,715,573` bytes) |

### Performance / Timing
*   **Launched Maps:** 613
*   **Launched Reduces:** 15
*   **Total Time (Maps):** `83,199,904 ms` (~23.1 hours cumulative CPU time)
*   **Avg Time per Map:** `~135.7 seconds` (2.2 minutes)
*   **Total Time (Reduces):** `25,606,667 ms` (~7.1 hours cumulative CPU time)
*   **Avg Time per Reduce:** `~1,707 seconds` (28.4 minutes)

---

## Job 2 (Step 2): Total Count (N) Calculation
*   **Job ID:** `job_..._0010`
*   **Wall-Clock Duration:** **16 Minutes** (23:09:50 - 23:26:29)
*   **Goal:** Calculate total words per decade (`N`).*   **Input:** Output of Step 1.
*   **Output:** `(decade) -> N` and tagged pairs.

### Statistics
| Metric | Value |
| :--- | :--- |
| **Map Input Records** | `240,464,868` |
| **Map Output Records** | `480,929,736` (Doubled due to emission of both `N` count and pair tag) |
| **Map Output Size** | `~12.0 GB` |
| **Combiner Used?** | No (`0` records) |
| **Reduce Input Records** | `480,929,736` |
| **Reduce Shuffle Size** | `~3.8 GB` |

### Performance / Timing
*   **Launched Maps:** 150
*   **Launched Reduces:** 16
*   **Total Time (Maps):** `15,080,573 ms`
*   **Avg Time per Map:** `~100.5 seconds` (1.7 minutes)
*   **Total Time (Reduces):** `6,518,053 ms`
*   **Avg Time per Reduce:** `~407 seconds` (6.8 minutes)

---

## Job 3 (Step 3): Join / PMI Score Calculation
*   **Job ID:** `job_..._0011`
*   **Wall-Clock Duration:** **17 Minutes** (23:26:34 - 23:43:49)
*   **Goal:** Join `N` with pairs and calculate PMI. (Note: IO metrics roughly match Job 2).

### Statistics
| Metric | Value |
| :--- | :--- |
| **Map Input Records** | `240,464,868` |
| **Map Output Records** | `480,929,736` |
| **Map Output Size** | `~15.6 GB` |
| **Combiner Used?** | No (`0` records) |
| **Reduce Input Records** | `480,929,736` |
| **Reduce Output Records** | `240,464,868` |
| **Reduce Shuffle Size** | `~5.3 GB` |

### Performance / Timing
*   **Launched Maps:** 170
*   **Launched Reduces:** 17
*   **Total Time (Maps):** `15,232,203 ms` (CPU Time)
*   **Avg CPU Time per Map:** `~89.6 seconds`
*   **Total Time (Reduces):** `6,677,070 ms` (CPU Time)
*   **Avg CPU Time per Reduce:** `~392 seconds`

---

## Job 4 (Step 4): Sorting & Filtering (Top-K)
*   **Job ID:** `job_..._0012`
*   **Wall-Clock Duration:** **10 Minutes** (23:43:54 - 23:53:31)
*   **Goal:** Sort by PMI and keep Top 100 per decade.*   **Input:** Output of Step 2 (Tagged records).
*   **Output:** Final PMI scores sorted/filtered (Top 100 per decade).

### Statistics
| Metric | Value |
| :--- | :--- |
| **Map Input Records** | `240,464,868` |
| **Map Output Records** | `240,464,868` |
| **Map Output Size** | `~7.2 GB` (`7,182,217,250` bytes) |
| **Combiner Used?** | No (`0` records) |
| **Reduce Input Records** | `240,464,868` |
| **Reduce Output Records** | `1,000` (Top 100 * 10 Decades) |
| **Reduce Shuffle Size** | `~4.7 GB` (`4,657,378,582` bytes) |

### Performance / Timing
*   **Launched Maps:** 142
*   **Launched Reduces:** 17
*   **Total Time (Maps):** `7,786,836 ms` (~2.16 hours cumulative CPU time)
*   **Avg Time per Map:** `~54.8 seconds` (0.9 minutes)
*   **Total Time (Reduces):** `2,675,372 ms` (~0.74 hours cumulative CPU time)
*   **Avg Time per Reduce:** `~157 seconds` (2.6 minutes)

---

## Total Process Summary
*   **Total Records Processed:** ~3.9 Billion (from S3)
*   **Total Shuffle I/O:** ~12.8 GB
*   **Total Map CPU Time:** ~31.5 Hours
*   **Total Reduce CPU Time:** ~10.7 Hours
*   **Effective Combiner Impact:** Reduced Step 1 network traffic by ~28 GB.
