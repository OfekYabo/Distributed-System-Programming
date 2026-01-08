# Implementation Plan - Assignment 2: Distributed System Programming

This assignment requires implementing a MapReduce algorithm on AWS EMR to extract collocations from the Google 2-grams dataset using Log Likelihood Ratio (LLR).

## Goal
Extract top-100 collocations per decade (English & Hebrew) sorted by LLR.

## Proposed Architecture
We will use a multi-step MapReduce job flow using Hadoop (Java).
Since we cannot assume memory capacity for joining counts, we will propagate partial counts through multiple MapReduce steps.

### Step 0: Configuration Layout & Arguments
*   **Goal**: robust parsing of CLI args/env vars to configure the job.
*   **Parameters**:
    *   `--lang`: [ENG/HEB]
    *   `--mode`: [LOCAL/CLOUD]
    *   `--data`: [FULL/PARTIAL] (Local allows only Partial)
    *   `--stop-words`: [REGULAR/EXTENDED]
    *   `--normalize`: [TRUE/FALSE]
*   **Logic**:
    *   Load `.env` if exists.
    *   Override with CLI args.
    *   **Validation**: Exit with strict error if any mandatory parameter is missing or invalid.
    *   Set up specific Input Paths (S3 vs Local).
    *   Pass constraints to Hadoop `Configuration`.

### Step 1: Aggregation & Filtering
*   **Goal**: Filter stop words, aggregate counts by decade for each bigram `w1 w2`.
*   **Input**: Google N-Grams (SequenceFile or Text).
*   **Mapper**:
    *   Parse line.
    *   Filter if `w1` or `w2` is a stop word.
    *   Extract Year -> Decade.
    *   Emit `Key: (Decade, w1, w2)`, `Value: count`.
*   **Combiner**:
    *   **Mandatory**. Sum counts locally. Reduces network bandwidth significantly.
*   **Reducer**:
    *   Sum counts for each `(Decade, w1, w2)`.
    *   Emit `Key: (Decade, w1, w2)`, `Value: c12`.
    *   **Counters**: Increment "Decade N" counter here (summing `c12` values). This is efficient because we process aggregated unique bigrams rather than every raw instance.

### Step 2: Calculate C1 (Count of w1)
*   **Goal**: Calculate total occurrences of `w1` in the decade (`c1`) and attach it to the bigram record.
*   **Input**: Output of Step 1 (`Key: (Decade, w1, w2)`, `Value: c12`).
*   **Mapper (Routing Only)**:
    *   Read Input.
    *   **Emit Distinct**: `Key: (Decade, w1, *)`, `Value: c12`. (For summing C1).
    *   **Emit Data**: `Key: (Decade, w1, w2)`, `Value: c12`. (For passing data).
*   **Pattern**: Order Inversion.
*   **Reducer**:
    *   Iterate stream.
    *   For `*`: Sum `c1`.
    *   For `w2`: Emit `Key: (Decade, w1, w2)`, `Value: (c12, c1)`.


### Step 3: Calculate C2, LLR & Filtering
*   **Goal**: Calculate total occurrences of `w2` (`c2`), then compute LLR.
*   **Input**: Output of Step 2 (`Key: (Decade, w1, w2)`, `Value: (c12, c1)`).
*   **Mapper (Routing Only)**:
    *   Read Input.
    *   **Emit Distinct**: `Key: (Decade, w2, *)`, `Value: c12`. (For summing C2).
    *   **Emit Data**: `Key: (Decade, w2, w1)`, `Value: (c12, c1)`. (For passing data).
*   **Pattern**: Order Inversion.
*   **Reducer**:
    *   Iterate stream.
    *   For `*`: Sum `c2`.
    *   For `w1`:
        *   Retrieve `N`.
        *   Calculate LLR.
        *   Emit `Key: (Decade, LLR)`, `Value: (w1, w2)`.


### Step 4: Sort & Top 100
*   **Goal**: Produce final Top 100 list per decade.
*   **Key**: `Pair<Decade, Double>` (Composite Key).
*   **Partitioner**: Partition ONLY by `Decade`. All pairs for "1990" go to the same reducer.
*   **Comparator**: Sort by `Decade` ASC, then `LLR` DESC.
*   **Mapper (Routing Only)**:
    *   Read Input.
    *   **Emit**: `Key: (Decade, LLR)`, `Value: (w1, w2)`.
    *   *Role*: Extracts the LLR score and moves it into the Composite Key to enable Secondary Sorting.
*   **Reducer**:
    *   Iterate values. Since keys are sorted by LLR descending:
    *   Write the first 100 pairs.
    *   Break loop. (Efficient, O(1) in Reducer logic).

## Project Structure
Standard Maven project structure:
```
Ass2/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── dsp/
│   │   │           └── ass2/
│   │   │               ├── Main.java        (Driver)
│   │   │               ├── steps/
│   │   │               │   ├── AggregationStep.java
│   │   │               │   ├── C1CalculationStep.java
│   │   │               │   ├── C2CalculationStep.java
│   │   │               │   └── SortStep.java
│   │   │               └── utils/
│   │   │                   ├── LLRUtils.java
│   │   │                   └── StopWords.java
│   │   └── resources/
│   │       ├── log4j.properties
│   └── test/
├── pom.xml
└── README.md
```

## Verification Plan

### Automated Tests
1.  **Unit Tests (JUnit)**:
    *   Test `LLRUtils` calculation formula against known values.
    *   Test Parsers for N-Gram format.
    *   Test StopWords filtering logic.
2.  **Local MR Testing (MRUnit or Hadoop Local)**:
    *   Run small sample data (created manually) through the pipeline locally to verify correct counts and LLR logic.

### Manual Verification
1.  **Local Hadoop Run**:
    *   Run the compiled jar on a local Hadoop installation (or pseudo-distributed mode) with `eng-stopwords.txt` and a small slice of N-Gram data.
    *   Verify output format and correctness of top items.
2.  **AWS EMR Dry Run**:
    *   Run on AWS EMR with a small subset of real data to verify S3 connectivity and permission/packaging.

## User Review Required
*   **Step Logic**: Confirm the multi-step approach (calculating c1, c2 by grouping) is acceptable. It scales well but requires shuffling data 3 times.
*   **Counter Propagation**: Using counters to find `N` is an efficient hack, but we must ensure counters are precise enough (they are `long`, so should be fine).
