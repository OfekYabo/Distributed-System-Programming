# Distributed System Programming - Assignment 2: Hadoop MapReduce

**Students:**
*   **Ofek Yabo** - ID: `209288588`
*   **Amit Zarhi** - ID: `208230235`

## 1. General Explanation
The goal of this assignment is to extract **Collocations** (pairs of words that appear together more frequently than by random chance) from the Google N-Gram dataset (Hebrew and English) using Hadoop MapReduce.

We calculate the **Log Likelihood Ratio (LLR)** for bigrams to identify meaningful phrases like "United States" or "High School" while filtering out noise and common pairings. Use of LLR is robust as it accounts for the relative frequency of individual words ($c_1, c_2$) and the total corpus size ($N$).

**Key Metrics Calculated:**
*   **$c_{12}$:** Count of the bigram $(w_1, w_2)$.
*   **$c_1$:** Count of the first word $w_1$ (as the first part of any bigram).
*   **$c_2$:** Count of the second word $w_2$ (as the second part of any bigram).
*   **$N$:** Total bigrams in the decade.

## 2. Implementation Details
The solution is a chain of 4 MapReduce jobs, designed for $O(1)$ memory usage per node.

### Step 1: Aggregation
*   **Input**: Raw Google N-Gram SequenceFiles.
*   **Map**: Filters stop-words (English/Hebrew), sanitizes text (removes non-alphanumeric), and emits `(Decade, w1, w2) -> count`.
*   **Combiner**: Aggregates counts locally to reduce network traffic.
*   **Reduce**: Sums counts for identical bigrams across the cluster. Output: `(Decade, w1, w2) -> c12`.

### Step 2: C1 Calculation (Order Inversion)
*   **Design**: Uses the "Order Inversion" pattern focused on $w_1$.
*   **Map**: For each `count` ($c_{12}$) from Step 1, emits two records:
    1.  `((Decade, w1, *), c12)` (For summing $c_1$)
    2.  `((Decade, w1, w2), c12)` (Data record)
*   **Partitioner**: Hashes by `(Decade, w1)`. This ensures both the special `*` record and all normal `w2` records for `w1` arrive at the **exact same Reducer**.
*   **Sort**: Custom comparator ensures `*` arrives first.
*   **Reduce**:
    *   Sums `C1` when it sees `*` records.
    *   Stores `C1` in a simple variable (O(1) memory).
    *   Attaches this `C1` to all subsequent `(w1, w2)` records.
    *   **Output**: `(Decade, w1, w2) -> (c12, c1)`
*   **Global N**: We effectively calculate global `N` (total bigrams per decade) here by summing all `c1` values (avoiding double counting) and passing them to Step 3 via Hadoop Global Counters (`Decade_N` group).

### Step 3: C2 Calculation & LLR
*   **Design**: Order Inversion focused on $w_2$.
*   **Map**: Emits `((Decade, w2, *), c12)` and `((Decade, w2, w1), {c12, c1})`.
*   **Partitioner**: Hashes by `(Decade, w2)` so all pairs ending in same word go to same reducer.
*   **Reduce**:
    *   Calculates `C2` when seeing `*`.
    *   Retrieves `N` from Context/Configuration (passed from Step 2).
    *   Computes **LLR** using $(c_{12}, c_1, c_2, N)$.
    *   **Output**: `(Decade, LLR) -> (w1, w2)`.

### Step 4: Sorting
*   **Map**: Inverses key to sorted standard key `(Decade, LLR)`.
*   **Partitioner**: Partitions by `Decade` (all records for 1990 go to one reducer).
*   **Sort**: Hadoop automatically sorts by LLR (Descending).
*   **Grouping**: Uses a **Grouping Comparator** to group strictly by `Decade`. This ensures `reduce()` is called used *once* per decade, with an iterator `values` containing all pairs sorted by LLR.
*   **Reduce**: Since `values` arrives sorted, we simply emit the first 100 records per decade and skip the rest.

## 3. Optimizations & Scalability
*   **Order Inversion**: We strictly avoided buffering values in `ArrayLists` in the Reducer. By emitting `*` records that arrive first, we calculate totals ($C_1, C_2$) in a streaming fashion. This ensures we never run out of heap space, even for words like "the" with millions of bigrams.
*   **Combiner**: Used in Step 1. As shown in the statistics report, this reduced network traffic by **~75%**.
*   **Global N Optimization**: Instead of a separate job, we calculate `N` as a "Side Effect" using Hadoop Global Counters in Step 2, efficiently passing it to Step 3.
*   **Sanitization**: We implemented a fast, manual character-check sanitization (iterating chars) rather than slow Regex compilation. This removes punctuation, ensures valid Hebrew/English words, and dramatically reduces noise in the output.

### Optimization Analysis: Potential Step 1 & 2 Merge
Looking at the statistics (Section 7), we observe that the **Reduce Input** and **Reduce Output** records in Step 1 are nearly identical (see table below).
*   *Meaning*: The Reducer in Step 1 does almost no aggregation because the **Combiner** effectively aggregated everything locally on the Mapper nodes.
*   *Implication*: Step 1's Reducer primarily just writes the data to HDFS, which Step 2 immediately reads back.
*   *Proposal*: We could optimize this by merging Step 1 and Step 2. The Step 1 Reducer could directly emit the Order Inversion pairs (`(w1, *)`, `(w1, w2)`) instead of writing the raw sums to disk. This would save **one entire MapReduce cycle** (I/O overhead, startup time), significantly improving performance.

## 4. Extras
*   **Resume Capability**: The job supports a `-DstartStep=X` argument. If Step 1 finishes but Step 2 fails, you can resume directly from Step 2 without reprocessing the massive raw dataset.
*   **Decade Filtering**: You can filter specific decades locally using `.env` variables (`START_DECADE`, `END_DECADE`) or in cloud using `-DstartDecade=[year] -DendDecade=[year]`.
*   **Sanitization**: Optimized input cleaning (O(N) pass, no Regex overhead).

## 5. How to Run

### Local Execution (Simulation)
Prerequisites: Java 8+, Maven.
*Note: You can use the `.env` file to set `START_DECADE`/`END_DECADE` for local runs too.*
```bash
mvn package
java -jar target/Ass2-4.0-SNAPSHOT.jar [input_path] [output_path]
# Language is automatically detected from input path (must contain 'eng' or 'heb')
```

### Cloud Execution (AWS EMR)
1.  Upload `Ass2-4.0-SNAPSHOT.jar` to your S3 bucket.
2.  Create an EMR Cluster (emr-5.x or higher).
3.  Add a **Custom JAR** Step:
    *   **JAR Location**: `s3://your-bucket/Ass2-4.0-SNAPSHOT.jar`
    *   **Arguments**:
        ```
        Optional: -DstartDecade=[year] -DendDecade=[year] -DstartStep=2
        Input Data Location
        Output Results Location
        ```
        **Example Hebrew:**

        ```
        com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data s3://ds-assignment-2-ofek/output/v4/heb_full
        ```
        **Example English:**
        ```
        com.dsp.ass2.Main s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data s3://ds-assignment-2-ofek/output/v4/eng_gb_full
        ```
4.  Monitor the Steps in the AWS Console.

---

## 6. Report: Statistics
**Efficiency Analysis:**
The following table demonstrates the massive bandwidth variance and optimization achieved by using a **Combiner** in Step 1.
*Without the combiner, we would have sent over 20 GB of raw data through the network shuffle.*

| Metric | English GB (Full Run) | Hebrew (Full Run) |
| :--- | :--- | :--- |
| **Map Input Pairs** | 2,000,106,933 | 252,069,581 |
| **Pairs Sent to Reducer (With Combiner)** | **165,800,730** | **26,123,857** |
| **Reduce Output Records** | 165,800,047 | 26,123,803 |
| **Ratio (In/Out)** | **~1.0** (Almost no Reducer work) | **~1.0** (Almost no Reducer work) |
| **Estimated Pairs (Without Combiner)** | 653,998,848 | 115,558,108 |
| **Reduction Ratio** | **~75%** | **~77%** |
| **Network Shuffle Size** | ~2.7 GB | ~414 MB |
| **Log File** | [eng_gb_full/syslog.log](data/EMR_output/eng_gb_full/syslog.log) | [heb_full/syslog.log](data/EMR_output/heb_full/syslog.log) |

*   **Pairs Sent to Reducer**: Actual `Reduce input records`.
*   **Estimated Pairs**: `Map output records` (This is high because Combiner runs *after* map but *before* network shuffle).
*   **Network Shuffle Size**: `Reduce shuffle bytes` (Actual data transferred between nodes).
*(Source: Hadoop "Map-Reduce Framework" Counters from run logs)*

## 7. Report: Analysis
**Output Data:**
*   **[English Output Folder](data/EMR_output/eng_gb_full)**
*   **[Hebrew Output Folder](data/EMR_output/heb_full)**

We manually inspected the output to identify "Good" (meaningful) and "Bad" (noise/error) collocations.
*Note: Before our sanitization step, the output contained many more artifacts (symbols, broken words). The current output is significantly cleaner due to the O(n) char-checker.*

### Hebrew Dataset
**[Hebrew Output Folder](data/EMR_output/heb_full)**

**Good Collocations (Strong Semantic Link):**
1.  **בית דין** - A fundamental legal institution.
2.  **ראש השנה** - Major Jewish holiday.
3.  **ארבעים שנה** - A distinct biblical period.
4.  **יצר הרע** - A core concept in Jewish thought.
5.  **אומות העולם** - Strong political/historical term.

**Bad Collocations (Errors/Noise):**
1.  **רוח הקרש** (Should be *רוח הקודש*) - **OCR Error** ('ר' instead of 'ד').
2.  **בגדי בהונה** (Should be *בגדי כהונה*) - **OCR Error** ('ב' instead of 'כ').
3.  **היח עסוק** (Should be *היה עסוק*) - **OCR Error** ('ח' instead of 'ה').
4.  **ואחד בימי** ("And one in the days of") - Grammatical fragment, not a standalone concept.
5.  **שאי אפשר** ("That is impossible") - Common phrase/stopword sequence that wasn't filtered.

### English (GB) Dataset
**[English Output Folder](data/EMR_output/eng_gb_full)**

**Good Collocations:**
1.  **Great Britain** - Country name, extremely strong collocation.
2.  **Robin Hood** - Iconic fictional character.
3.  **Holy Ghost** - Religious term (Trinity).
4.  **Pater noster** - Latin prayer ("Our Father"), distinct phrase.
5.  **Merry Wives** - Cultural reference (Shakespeare).

**Bad Collocations (Mostly OCR/Old English Script Issues):**
1.  **fo far** (Should be *so far*) - **OCR Error** ('f' instead of long 's').
2.  **fame time** (Should be *same time*) - **OCR Error** ('f' instead of long 's').
3.  **fir ft** (Should be *first*) - **OCR Error** (Split word + 'f'/'s' error).
4.  **mo ft** (Should be *most*) - **OCR Error** (Split word).
5.  **thou art** - Very frequent grammatical pair (Subject-Verb), often appearing due to old English texts. While not "noise", it's not a semantic idiom like "Red Cross".
