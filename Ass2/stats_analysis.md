
# Hadoop Job Statistics Analysis

## English 10 Decades (1990-2000)

### Step 1 Analysis (Already Provided)
*   **Sent from Mappers:** 1,139,756,121
*   **Sent to Reducers:** 240,480,520 (79% reduction via Combiner)

### Step 2 Analysis (Already Provided)
*   **Sent from Mappers:** 480,929,736
*   **Sent to Reducers:** 480,929,736 (No Combiner used)

### Step 3 Analysis (From New Log)

This step performs the final PMI calculation and filtering.

#### 1. Key-Value Pairs
*   **Sent from Mappers (Map Output Records):** `480,929,736`
    *   *Why?* Step 3 reads the output of Step 2, which had emitted all tagged records for calculation.
*   **Sent to Reducers (Reduce Input Records):** `480,929,736`

#### 2. Data Size
*   **Map Output Bytes:** `16,781,302,892` bytes (~15.6 GB)
*   **Reduce Shuffle Bytes:** `5,715,225,004` bytes (~5.3 GB)

#### 3. Aggregation Efficiency
*   **Combiner Input Records:** `0`
*   **Combiner Output Records:** `0`
*   **Analysis:** Similar to Step 2, **no Combiner occurred** here. The mapper likely just "passed through" the data (or keyed it by decade+collocation for the final merge) so the reducer could calculate the final PMI.
*   **Note:** Since the Reducer Output is `240,464,868` (roughly half of the input), it confirms that the Reducer merged the paired messages (one line for count, one for N) back into a single result line per collocation.

### Summary
*   **Total Map Output Bytes (Step 1+2+3):** ~60 GB processed across the cluster.
*   **Total Shuffle Bytes (Network):** ~12.8 GB transferred.
