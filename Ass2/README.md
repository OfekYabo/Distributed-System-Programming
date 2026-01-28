# Distributed System Programming - Assignment 2: Hadoop MapReduce

**Students:**
*   **[Ofek Yabo]** - ID: `[209288588]`
*   **[Amit Zarhi]** - ID: `[208230235]`

## 1. General Explanation
The goal of this assignment is to extract **Collocations** (pairs of words that appear together more frequently than by random chance) from the Google N-Gram dataset (Hebrew and English) using Hadoop MapReduce.

We calculate the **Log Likelihood Ratio (LLR)** for bigrams to identify meaningful phrases like "United States" or "High School" while filtering out noise and common pairings. Use of LLR is robust as it accounts for the relative frequency of individual words ($c_1, c_2$) and the total corpus size ($N$).

**Key Metrics Calculated:**
*   **$c_{12}$:** Count of the bigram $(w_1, w_2)$.
*   **$c_1$:** Count of the first word $w_1$ (as the first part of any bigram).
*   **$c_2$:** Count of the second word $w_2$ (as the second part of any bigram).
*   **$N$:** Total bigrams in the decade.

## 2. Implementation Details
The solution is a chain of 4 MapReduce jobs, designed for $O(1)$ memory usage per node (no loading huge hashmaps into RAM).

### Step 1: Aggregation
*   **Input**: Raw Google N-Gram SequenceFiles.
*   **Map**: Filters stop-words (English/Hebrew), sanitizes text (removes non-alphanumeric), and emits `(Decade, w1, w2) -> count`.
*   **Combiner**: Aggregates counts locally to reduce network traffic.
*   **Reduce**: Sums counts for identical bigrams across the cluster. Output: `(Decade, w1, w2) -> c12`.

### Step 2: C1 Calculation (Order Inversion)
*   **Design**: Uses the "Order Inversion" pattern.
*   **Map**: For each `(w1, w2)`, emits two records:
    1.  `(Decade, w1, *) -> count` (Special "Counter" record)
    2.  `(Decade, w1, w2) -> count` (Data record)
*   **Sort**: Ensures `*` comes before any word.
*   **Reduce**: sums `C1` when it sees `*`, stores it in a simple long variable, and attaches this `C1` to all subsequent `(w1, w2)` records.
*   **Global N**: We also successfully calculate the global `N` counters per decade here and pass them to Step 3 via Hadoop Counters.

### Step 3: C2 Calculation & LLR
*   **Design**: Order Inversion focused on $w_2$.
*   **Map**: Emits `(Decade, w2, *)` and `(Decade, w2, w1)`.
*   **Reduce**:
    *   Calculates `C2` when seeing `*`.
    *   Retrieves `N` from Context/Configuration.
    *   Computes **LLR** using $(c_{12}, c_1, c_2, N)$.
    *   Emits `(Decade, LLR) -> (w1, w2)`.

### Step 4: Sorting
*   **Map**: Inverses key to sorted standard key.
*   **Sort**: Hadoop sorts by LLR (Descending).
*   **Reduce**: Emits only the Top 100 pairs per decade.

## 3. Optimizations & Scalability
*   **Order Inversion**: We strictly avoided buffering values in `ArrayLists` in the Reducer. By emitting `*` records that arrive first, we calculate totals ($C_1, C_2$) in a streaming fashion. This ensures we never run out of heap space, even for words like "the" with millions of bigrams.
*   **Combiner**: Used in Step 1. As shown in the statistics report, this reduced network traffic by **~75%**.
*   **Global N Optimization**: Instead of a separate job, we calculate `N` as a "Side Effect" using Hadoop Global Counters in Step 2, efficiently passing it to Step 3.
*   **Sanitization**: We implemented regex-based input sanitization to remove punctuation and noise before processing.

## 4. Extras
*   **Resume Capability**: The job supports a `-DstartStep=X` argument. If Step 1 finishes but Step 2 fails, you can resume directly from Step 2 without reprocessing the massive raw dataset.
*   **Decade Filtering**: Optional arguments to run only specific decades (e.g., `start=1990 stop=2000`).
*   **Sanitization**: Input text is cleaned of non-alphanumeric characters to improve quality.

## 5. How to Run

### Local Execution (Simulation)
Prerequisites: Java 8+, Maven.
```bash
mvn package
java -jar target/Ass2-4.0-SNAPSHOT.jar [input_path] [output_path] [language(heb/eng)]
```

### Cloud Execution (AWS EMR)
1.  Upload `Ass2-4.0-SNAPSHOT.jar` to your S3 bucket.
2.  Create an EMR Cluster (emr-5.x or higher).
3.  Add a **Custom JAR** Step:
    *   **JAR Location**: `s3://your-bucket/Ass2-4.0-SNAPSHOT.jar`
    *   **Arguments**:
        ```
        s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-gb-all/2gram/data
        s3://your-bucket/output/eng_run_1
        eng
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
| **Estimated Pairs (Without Combiner)** | 653,998,848 | 115,558,108 |
| **Reduction Ratio** | **~75%** | **~77%** |
| **Network Shuffle Size** | ~2.7 GB | ~414 MB |

*(Source: Hadoop "Map-Reduce Framework" Counters from run logs)*

## 7. Report: Analysis
We manually inspected the output to identify "Good" (meaningful) and "Bad" (noise/error) collocations.

### Hebrew Dataset
**Good Collocations (Strong Semantic Link):**
1.  **בית דין** (Court of Law) - A fundamental legal institution.
2.  **ראש השנה** (Rosh Hashanah) - Major Jewish holiday.
3.  **ארבעים שנה** (Forty Years) - A distinct biblical period.
4.  **יצר הרע** (Evil Inclination) - A core concept in Jewish thought.
5.  **אומות העולם** (Nations of the World) - Strong political/historical term.

**Bad Collocations (Errors/Noise):**
1.  **רוח הקרש** (Should be *רוח הקודש*) - **OCR Error** ('ר' instead of 'ד').
2.  **בגדי בהונה** (Should be *בגדי כהונה*) - **OCR Error** ('ב' instead of 'כ').
3.  **היח עסוק** (Should be *היה עסוק*) - **OCR Error** ('ח' instead of 'ה').
4.  **ואחד בימי** ("And one in the days of") - Grammatical fragment, not a standalone concept.
5.  **שאי אפשר** ("That is impossible") - Common phrase/stopword sequence that wasn't filtered.

### English (GB) Dataset
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
5.  **thou art** - While frequent, this is a grammatical subject-verb pair (stopword candidates) rather than a "semantic" collocation like "New York".
