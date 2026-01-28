# Assignment 2: Detailed Problem Analysis & Q&A

## 1. General Explanation of the Task
**Goal**: The objective is to identify "Collocations" (pairs of words that appear together more intuitively than random chance, e.g., "New York", "High School") from a massive dataset of text from books (Google N-Grams).
**Data**: You are working with **Bigrams** (pairs of words) and **Unigrams** (single words) data collected by Google from scanned books over decades.
**Tools**:
*   **Hadoop**: The software framework that allows for distributed processing of large data sets across clusters of computers using simple programming models.
*   **AWS EMR (Elastic MapReduce)**: A managed service for running Hadoop on Amazon's cloud servers (EC2 instances).
**Limitations**:
*   **Memory**: You cannot assume you can load all data into RAM. For example, you cannot create a HashMap of all word counts in memory.
*   **Redundancy**: You must avoid generating unnecessary key-value pairs to keep network traffic and storage low.

## 2. What We Actually Need to Calculate
We need to calculate the **Log Likelihood Ratio (LLR)** for every bigram $(w_1, w_2)$ for each decade.
To calculate LLR, we need 4 key components for each bigram $w_1 w_2$ in a specific decade:
1.  **$c_{12}$**: The count of the bigram "$w_1 w_2$" (how many times they appear together).
2.  **$c_1$**: The total count of $w_1$ (how many times $w_1$ appears as the first word in any bigram, or arguably in the whole corpus).
3.  **$c_2$**: The total count of $w_2$ (how many times $w_2$ appears as the second word).
4.  **$N$**: The total number of all bigrams (or words) in that decade.

Once we have these four numbers ($c_{12}, c_1, c_2, N$) for a specific pair, we plug them into the provided formula to get a score. The higher the score, the stronger the collocation.

## 3. Data Sets: Bigrams and Unigrams
**N-Gram**: An N-gram is a contiguous sequence of *n* items from a given sample of text.
*   **Unigram (1-gram)**: A single word. (e.g., "University", "is").
*   **Bigram (2-gram)**: A sequence of two adjacent words. (e.g., "University is").

The datasets provided are stored on Amazon S3. They are massive lists of these N-grams along with the year they appeared and how many times they appeared.
*   *English/Hebrew Bigrams*: The core data source for finding finding bigrams ($w_1, w_2$).
*   *Unigrams*: Can be used to find absolute counts ($c_1, c_2$) if needed, though often it's consistent to calculate $c_1$ and $c_2$ by summing up bigram counts to ensure consistency within the bigram dataset provided.

## 4. What is Hadoop?
**Hadoop** is an open-source framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models.
*   **The "Map" Phase**: Takes input data and converts it into a set of intermediate Key/Value pairs. (e.g., "Split a line of text into words").
*   **The "Reduce" Phase**: Takes the intermediate Key/Value pairs and aggregates/processes them to produce the final output. (e.g., "Sum up the counts for each word").

**Cloud vs. Local**:
*   **Cloud (AWS EMR)**: You define a "Job Flow". AWS spins up multiple virtual computers (Nodes), installs Hadoop on them, distributes your data and exact code to them, runs it, and saves the results to S3. You pay for the time the machines are running.
*   **Local**: You simulate a cluster on your own laptop. It runs the same "Map" and "Reduce" logic but sequentially or with limited parallelism on your own CPU. Great for debugging logic without paying money.

## 5. What is an N-gram File?
The specific N-gram files provided by Google are in a binary format called **SequenceFile**.
*   **Compression**: They are compressed using **LZO** (Lempel-Ziv-Oberhumer) to save space.
*   **Counting N**:
    *   We do **not** emit `N` as a key-value pair to the Reducer.
    *   Instead, we use **Hadoop Global Counters** in the **Reducer**.
    *   *Why?* It is more efficient to increment counters after aggregation (fewer increments) than for every raw record in the Mapper.
    *   The Driver (Main class) reads these counters after Step 1 finishes.
*   **Content**: They contain Key-Value pairs where the Key might be the Bigram text and the Year, and the Value is the count.
*   **Reading it**: You cannot read this with a normal `FileReader`. You need to use Hadoop's `SequenceFileInputFormat` class which knows how to handle the binary structure and decompression automatically.

## 6. How to Develop and Test Locally
1.  **Install Hadoop**: Install a single-node Hadoop on your machine (Win/Linux). Or use Hadoop libraries in your Java project just for "Local Job Runner" mode (often sufficient for logic testing).
2.  **Small Data**: Create a tiny text file manually that mimics the input (e.g., "high school 1990 10\nhigh school 1991 12").
3.  **Local Run**: Configure your Java Main class to use "local" mode when no arguments are passed or when a specific flag is set.
4.  **Verify Logic**: Step through your Mappers and Reducers using a debugger (IntelliJ/Eclipse) to ensure your counters and aggregation logic work.
5.  **Unit Tests**: Use **MRUnit** or standard JUnit tests to verify individual Mapper and Reducer classes without running the full Hadoop stack.

## 7. Limitations & Transition to Cloud
**Local to Cloud Transformation**:
*   **Build**: You compile your code into a single `.jar` file (Fat Jar usually not needed if using standard libraries, but dependencies must be included if non-standard).
*   **Upload**: Upload the `.jar` to an S3 bucket.
*   **Run**: Use the AWS Console or CLI (or SDK code as shown in the example) to "Run Job Flow" pointing to that S3 jar.

**Lab AWS User Limitations**:
*   **Cost**: Use **Spot Instances** or small On-Demand instances (M4.Large). **TERMINATE** the cluster immediately after the job finishes.
*   **Permissions**: You might have restricted policies. Stick to S3 for storage and EMR for compute.
*   **Cleanup**: **Always** delete the cluster and large S3 temporary files. Logs can be huge.

## 8. Requirements for Full Grading
*   **Correctness**: The top 100 list must closely match expected results for known collocations.
*   **Stop Words**: You MUST filter out stop words (e.g., "the", "and", "is") based on the provided lists. If a bigram contains a stop word, ignore it.
*   **Scalability**: Do not store massive lists in Java Collections. Let Hadoop's Sort/Shuffle phase do the heavy lifting of grouping data.
*   **Reports**:
    *   **Statistics**: Calculate and report map-output records vs reduce-input records (demonstrating the effect of "Combiners").
    *   **Bad/Good Examples**: Manually analyze the output file and pick examples.
*   **Efficiency**: Use a **Combiner** in Step 1 to aggregate counts locally before sending them over the network. This is crucial for performance and grading.

## 9. Map, Combiner, Reduce: Methods & Limitations
*   **The Mapper**:
    *   *Role*: Parses raw input, filters data (e.g., stop words, years), and emits intermediate Key-Value pairs.
    *   *Limitation*: Works on a single record at a time. No knowledge of other records.
*   **The Combiner** (Mini-Reducer):
    *   *Role*: Runs locally on the Mapper node to aggregate data before sending it over the network. Crucial for bandwidth efficiency.
    *   *Limitation*: **Must be Associative and Commutative**.
        *   *Allowed*: Sum `(a+b)+c = a+(b+c)`, Max, Min.
        *   *Not Allowed*: Average `Avg(Avg(a,b), c) != Avg(a,b,c)`. You cannot calculate the final average in a combiner because it doesn't know the weights of the partial averages. You *can* emit `(sum, count)` tuples and sum those, but you cannot emit the computed average.
        *   *Not Allowed*: Median, Unique Count (unless using HyperLogLog or preserving the set).
*   **The Reducer**:
    *   *Role*: Receives all values for a specific Key. Performs final aggregation and computation.
    *   *Capability*: Can do non-associative operations (Average, LLR Calculation) because it sees the *entire* list of values for that key.

## 10. Ideas for Extras (Bonus/Excellence)
*   **Smart Stop Words**: The provided list is basic. Integrating a better, more comprehensive stop-word list (e.g., from Lucene or NLTK) to clean up results further.
*   **Stemming/Lemmatization**: Normalize words (e.g., "cat" and "cats" treated as same) for better counts (though might be out of scope for strict bigram matching).
*   **Robustness**: Add try-catch blocks to handle malformed input lines without crashing the whole job (use Hadoop *Counters* to track skipped bad records).
*   **Advanced Parameterization & Configuration**: The system must support flexible execution via command-line arguments or environment variables (`.env`).
    *   **Language**: Toggle between `Hebrew` and `English` (determines input S3 paths and specific stop-words).
    *   **run_mode**: `Local` vs `Cloud`.
        *   *Local*: Uses local filesystem paths and local Hadoop runner.
        *   *Cloud*: Uses S3 paths and submits steps to EMR.
    *   **Dataset Scope**: `Full` vs `Partial`.
        *   *Local Usage*: Must be restricted to `Partial` (sample) data only to avoid memory/storage crash.
        *   *Cloud Usage*: Support `Partial` for cost-effective debugging (Spot instances) coverage, and `Full` for final graded run.
    *   **Stop Words Strategy**: `Regular` (assignment filters) vs `Extended` (aggressive filtering).
    *   **Normalization**: `Regular` (exact string match) vs `Normalized` (canonical representation/stemming).

*   **Combiner Optimization**: Explicitly analyzing and proving the bandwidth saved by using Combiners (using Hadoop Counters).

## 11. Detailed Workflow & Sorting Strategy
The solution relies on a chain of MapReduce steps to propagate counts without memory saturation.

### Step 1: Aggregation
*   **Input**: Raw n-grams.
*   **Map**: Filter stop-words. Emit `(Decade, w1, w2) -> count`.
*   **Combiner**: Sum counts locally.
*   **Reduce**: Sum global counts. Output `(Decade, w1, w2) -> c12`.
*   **Side Effect**: Increment global counters for `N` per decade.

### Step 2: C1 Calculation
*   **Pattern: Order Inversion (The "Pairs" Pattern)**
    *   *Problem*: We cannot buffer all `w2`s for a `w1` in memory.
    *   *Solution*: We emit a special symbol `*` to ensure the total count arrives *first*.
*   **Map**:
    *   Input: `(1990, high, school) -> 30`, `(1990, high, quality) -> 5`.
    *   Emits:
        1.  `((1990, high, *), 30)`  <-- Special "Header" record
        2.  `((1990, high, school), 30)`
        3.  `((1990, high, *), 5)`   <-- Special "Header" record
        4.  `((1990, high, quality), 5)`
*   **Partitioner**: Hash by `(Decade, w1)`. All "high" records go to same Reducer.
*   **Sort**: Custom Comparator ensures `(high, *)` comes before `(high, school)`.
*   **Reducer**:
    *   Input Stream: `(high, *), (high, *), (high, quality), (high, school)...`
    *   Logic:
        *   While key is `(high, *)`: Sum `C1 += value`. (Do not emit).
        *   Current `C1` is now fully calculated (e.g., 35).
        *   While key is `(high, word)`: Emit `(1990, high, word) -> (c12=val, c1=35)`.
    *   *Result*: We processed the whole group **without buffering**. This is valid and scalable.

### Step 3: C2 Calculation (Focus on w2)**
*   **Pattern**: Order Inversion (Same as Step 2).
*   **Map**:
    *   Input: `(1990, high, school) -> (30, 35)`.
    *   Emit:
        1.  `((1990, school, *), 30)`      <-- For calculating C2
        2.  `((1990, school, high), (30, 35))` <-- Data carrying C1
*   **Reducer**:
    *   Logic:
        *   While key is `(school, *)`: Sum `C2 += value`.
        *   Current `C2` is calculated (e.g., 70).
        *   While key is `(school, high)`:
            *   We have `c12` (30) from value.
            *   We have `c1` (35) from value.
            *   We have `c2` (70) from local variable.
            *   We have `N` from Context.
            *   Calculate LLR.
    *   Emit: `(Decade, LLR) -> (w1, w2)`.



**Step 4: Sorting**
*   **Map**:
    *   Input: `((1990, 125.4), (high, school))`.
    *   Action: Identity Mapper (or simply extraction).
    *   Emits: `((1990, 125.4), (high, school))`.
*   **Combiner**:
    *   *Not Applicable*. The keys are unique (Score is continuous), so very little aggregation opportunity.
*   **Partitioner**:
    *   Partitions by `Decade` only. All `1990` go to Reducer 0.
*   **Sort (Shuffle)**:
    *   Hadoop Framework automatically sorts the keys. `(1990, 125.4)` comes before `(1990, 110.2)`.
*   **Reducer**:
    *   Input: Stream of sorted keys.
    *   Action: Counter `i = 0`.
    *   Loop:
        *   Write record.
        *   `i++`
        *   If `i >= 100` break/return.
    *   Output: Top 100 records per decade.

## 13. Q&A: Inputs, Scope, and Data Organization
**Q: Global Top 100 vs. Decade Top 100?**
*   **A: Per Decade.** The assignment explicitly says "produces the list of top-100 collocations **for each decade**". The output should likely look like folders `output/1990`, `output/2000`, etc., or a single file where lines are prefixed with the decade.

**Q: How are input files organized?**
*   **A**: The S3 path `.../2gram/data` is a **folder** containing many files (e.g., `part-00001`, `part-00002`).
    *   Google N-Grams are typically sorted alphabetically by the bigram.
    *   One file contains **many years** (e.g., all lines for "apple ..." across all years might be in one file).
    *   They are **NOT** separated by decade. Your Mapper must parse the year and bucket it into a decade (e.g., 1995 -> 1990s).

**Q: Bigrams vs. Unigrams?**
*   **A**: We will use **only Bigrams**.
    *   *Why?* Calculating $c_1$ (count of "high") by summing all bigrams starting with "high" (Step 2) is safer and ensures consistency with the $c_{12}$ counts we act on.
    *   Joining with the separate Unigram dataset adds complexity (synchronizing separate files) without much benefit for this specific LLR task.

## 14. Data Distribution & Routing Logic
**Q: How much data does each Mapper handle?**
*   **Input Splits**: Hadoop splits large files into logical blocks called **InputSplits**.
*   **Size**: Default is typically **128 MB** (aligned with HDFS block size).
*   **Granularity**: A Mapper does *not* necessarily process a whole file. If "google-ngrams-part-1.txt" is 1GB, it will be broken into ~8 splits, processed by 8 separate Mapper instances in parallel.
*   **Record Integrity**: Hadoop's `InputFormat` ensures records (lines) are not broken in half.

**Q: Can we remove Step 2 Mapper?**
*   **No.** Step 1 output is grouped by `(Decade, w1, w2)`. Step 2 requires grouping by `(Decade, w1)`.
*   Even if Step 1 emits special records, they are on the "wrong" node.
*   **The Shuffle**: We *must* have a Mapper (even an identity one) to read the data, extract the new key `(Decade, w1)`, and let Hadoop shuffle it to the correct Reducer. You cannot "send" data from Reducer 1 to Reducer 2 without a Shuffle (Map phase) in between.

**Q: How do Mappers send data to Reducers? (The Shuffle Phase)**
*   **Partitioning**: Mappers emit generic `(Key, Value)` pairs. The destination Reducer is decided by:
    *   `ReducerID = ` `Key.hashCode() % NumberOfReducers`.
*   **Guarantee**: All occurrences of the **Same Key** (e.g., "1990 high school") will produce the same Hash, thus arriving at the **Same Reducer**.
*   **Sorting**: Before reaching the Reducer's `reduce()` function, the framework merges and sorts all keys.
*   **Step 4 Special Case**: We use a `Custom Partitioner`. We hash *only* the Decade part of the key.
    *   Key: `(1990, 125.4)` -> Partition based on `hash(1990)`.
    *   Result: All records for `1990` go to Reducer X, regardless of the Score. This allows Reducer X to see the *entire* sorted list for 1990 and pick the top 100.

## 15. Advanced Visualization: "Order Inversion"
**Q: Why was the original Step 2 description potentially memory-heavy?**
*   **Analysis**: If we group by `w1` and just send `[(school, 30), (quality, 5)]`, the Reducer must iterate ALL of them to sum 35, and *then* iterate ALL of them again to emit. This requires holding them in RAM (buffering). If "the" has 1 million bigrams, we crash.

**Q: How does "Order Inversion" solve this?**
*   **Mechanism**: We artificially inject a special record `(w1, *)` that carries the count.
*   **Sorting**: We guarantee `(w1, *)` arrives **before** `(w1, w2)`.
*   **Stream**:
    1.  Reducers sees `(high, *)` -> Adds to `current_c1_sum`.
    2.  Reducers sees `(high, *)` -> Adds to `current_c1_sum`.
    3.  ... (All stars finished). `current_c1_sum` is now correct (e.g., 35).
    4.  Reducer sees `(high, school)` -> Writes `(high, school, c12, c1=35)`.
    5.  Reducer sees `(high, quality)` -> Writes `(high, quality, c12, c1=35)`.
*   **Memory**: We only stored **one integer** (`current_c1_sum`). 0 RAM usage for the list.

**Q: Can we merge Step 1 and Step 2?**
*   **Yes!** Using Order Inversion, Step 1 can emit `((decade, w1, *), 1)` and `((decade, w1, w2), 1)`.
*   A Combiner can sum these locally.
*   The Reducer uses the Order Inversion logic to summing Global C1 and then emitting Step 2 format directly.
*   **Tradeoff**: It increases the complexity of Key types (`Text` vs `Composite`) in the very first step. For clarity in this assignment plan, we kept them separate, but merging them is a *valid and excellent optimization*.


## 16. Deep Dive: Reducer Memory Internals

**Q: Does the Reducer hold all Keys in memory?**
*   **NO.** It holds **exactly ONE Key** at a time.
*   **The Framework Loop**: Hadoop's internal runner does roughly this:
    ```java
    while (rawStream.hasMore()) {
        Key currentKey = rawStream.readKey();
        Iterator values = new ValueIterator(rawStream);
        reduce(currentKey, values); // <--- YOUR CODE RUNS HERE
    }
    ```
*   **Sequential Processing**: You process Key A. You finish. You return. The framework discards Key A. It reads Key B. It calls you again.
*   **Implication**: You cannot "go back" to Key A. You cannot "peek" at Key B while processing Key A.

**Q: Does the Reducer hold all Values for a Key in memory?**
*   **NO.** This is the second critical concept.
*   **The Lazy Iterator**: The `values` iterator passed to your `reduce` method is *lazy*.
    *   It pulls data from the disk/network buffer one by one.
    *   When you call `values.next()`, it overwrites the previous value object (memory reuse) to save RAM.
    *   **Implication**: If you want to see the list *twice* (sum first, then emit), you *must* manually copy every value into a Java `ArrayList`. **This is where you crash if the list is big.**

**Q: How Order Inversion saves us?**
*   Because `(w1, *)` comes first (guaranteed by sorting):
    1.  The framework calls `reduce(w1, iterator)`.
    2.  You call `iterator.next()`. It is `*`. You update `total`.
    3.  You call `iterator.next()`. It is `*`. You update `total`.
    4.  You call `iterator.next()`. It is `school`. You now have the `total` ready. You emit.
    5.  You keep going until iterator matches `false`.
*   **Memory**: We only stored **one integer**. We streamed the rest.


## 17. Decision Matrix: Merged vs. Separate Steps
**The Dilemma**: Merge Step 1 & 2 (save I/O, cost) vs. Keep Separate (simplicity, safety).

| Feature | **Separate Steps (Recommended)** | **Merged Step 1+2** |
| :--- | :--- | :--- |
| **Complexity** | Low. Step 1 is standard WordCount. Step 2 is standard Join. | High. Requires Composite Key `(Decade, w1, w2)` handling `*` in the very first step. |
| **Debuggability** | High. You can check `output/step1` to see if counts are correct before solving LLR. | Low. If LLR is wrong, is it the counting logic or the join logic? Hard to say. |
| **Network I/O** | 3 Shuffles. (Step 1->2 is extra). | 2 Shuffles. Saves writing intermediate counts to S3. |
| **Data Volume** | Standard. | Slightly Higher. Mappers emit `(w1, *)` and `(w1, w2)`. Combiners mitigate this effectively ($O(1)$ overhead). |
| **Memory Risk** | **Safe** (with Order Inversion in Step 2). | **Safe** (with Order Inversion). |
| **Verdict** | **Stick to Separate.** The assignment is complex enough. Saving one step isn't worth the debugging nightmare if distinct counting fails. | **bonus/Excellence**. Do this only if you have extra time and everything else works perfectly. |

## 18. Critical Concept: The "Shuffle Barrier" (Why Mapper 2 is Mandatory)
**Q: Can Reducer 1 send data directly to Reducer 2?**
*   **NO.** In MapReduce, Reducers **always write to Disk (HDFS/S3)**. They terminate when finished. They do not distinct "stream" to the next job.
*   **Implication**: Step 2 starts with a fresh set of Mappers that read those files from Disk.

**Q: Can we emit the `*` in Reducer 1?**
*   **Yes, but it doesn't help.** Even if Reducer 1 writes `((1990, high, *), 30)` to the disk, that file sits on Node A (or S3).
*   The record `((1990, high, quality), 5)` might sit on Node B.
*   **The Problem**: To sum them up, **they must meet on the same machine** (Node C).
*   **Start of Step 2**:
    *   Mapper 2 reads the files.
    *   It looks at the key `high`.
    *   It says "Ah, `high` belongs to Node C".
    *   **The Shuffle Phase moves the data**.
*   **Conclusion**: You **Key** triggers the routing. Without a Mapper to read the disk and emit keys, the data stays stuck on the wrong nodes. The Mapper is the "Router".
