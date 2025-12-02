# Transactional Vector Store Design (IVF on SOP)

## 1. Core Problem: Indexing High-Dimensional Vectors
We explored why standard B-Tree indexing fails for raw vectors:
*   **Unit Sphere Problem**: AI embeddings are normalized (Length = 1.0). Sorting by "Magnitude" (Distance from Origin) puts all items in the same bucket.
*   **Lexicographical Failure**: Sorting by `Vector[0]`, then `Vector[1]` fails because two similar vectors might have slightly different first dimensions (e.g., `0.49` vs `0.50`), placing them in far-apart branches of the B-Tree.

## 2. The Solution: Inverted File Index (IVF)
Instead of indexing raw vectors, we index **Clusters**.
1.  **Centroids**: We define (or learn) $K$ representative points ("Centroids") in the vector space (e.g., 1,000 centroids).
2.  **Assignment**: Every data vector is assigned to its closest Centroid.
    *   *Calculation*: `ID = FindClosestCentroid(InputVector).ID`
3.  **Indexing**: The B-Tree Key is derived from this assignment.

## 3. Architecture: Dual B-Tree System
We utilize two separate SOP B-Trees to manage this structure efficiently and transactionally.

### B-Tree #1: The Directory (Centroids)
*   **Purpose**: Maps `CentroidID` -> `Centroid` struct.
*   **Size**: Small (e.g., 1k - 10k items). Fits in RAM.
*   **Key**: `int` (Centroid ID).
*   **Value**: `Centroid` struct containing:
    *   `Vector`: `[]float32` (The vector coordinates of the center).
    *   `VectorCount`: `int` (Number of vectors assigned to this cluster).
*   **Usage**: Used during Ingestion (to find where to insert) and Search (to find which buckets to scan).

### B-Tree #2: The Library (Vectors)
*   **Purpose**: Stores the actual item vectors, grouped by cluster AND ordered by distance-to-center.
*   **Size**: Huge (Billions of items). Disk-based.
*   **Key**: `CentroidID` (Primary) + `DistanceToCentroid` (Secondary) + `ItemID` (Tertiary).
    *   *Optimization*: By including `DistanceToCentroid` in the key, we can perform Range Scans within the bucket.
    *   *Triangle Inequality*: If `Query` is distance $D$ from `Centroid`, we only need to scan items with distance $[D-\epsilon, D+\epsilon]$ from `Centroid`.
*   **Value**: `[]float32` (The raw vector).
    *   *Note*: We do NOT store the full content here to keep the index compact and cache-friendly.

### B-Tree #3: The Content Store
*   **Purpose**: Stores the full item data (JSON, Text, Metadata).
*   **Key**: `ItemID` (e.g., "doc-101").
*   **Value**: `string` (JSON blob).
*   **Usage**: Retrieved only for the final top-K results.

### B-Tree #4: The Lookup Store
*   **Purpose**: Maps a dense integer sequence to Item IDs.
*   **Key**: `SequenceID` (int).
*   **Value**: `ItemID` (string).
*   **Usage**: Enables efficient random sampling (e.g., for training K-Means) by picking random integers.

### B-Tree #5: The TempVectors Store
*   **Purpose**: Temporarily stores vectors during the initial build phase before they are assigned to centroids.
*   **Key**: `ItemID` (string).
*   **Value**: `[]float32` (Vector).
*   **Usage**: Used in `BuildOnceQueryMany` mode to hold data until K-Means is trained.

### Flow A: The "Search" Flow (Query)
*   **Goal**: Find similar items.
*   **Direction**: `Query` -> `Centroids` -> `Library` -> `Content`.
    1.  **Directory**: Find closest centroids.
    2.  **Library**: Scan candidate keys (e.g., `{Centroid: 5, Dist: 0.2}`).
    3.  **Content**: Fetch full record using `ItemID` found in Library.

### Flow B: The "Management" Flow (Get/Delete/Update)
*   **Goal**: Manage a specific item by ID.
*   **Direction**: `ID` -> `Content` -> `Library`.
    1.  **Content**: Look up `ItemID` ("doc-101") to get metadata.
    2.  **Extract Address**: Read `_centroid_id` (5) and `_distance` (0.2) from metadata.
    3.  **Library**: Use the full key `{5, 0.2, "doc-101"}` to locate and delete/update the vector.

## 7. Prototype Implementation Plan
*   Define `CentroidStore` struct (wraps B-Tree #1).
*   Define `DataStore` struct (wraps B-Tree #2).
*   Implement `Ingest(vector)`: Find Centroid -> Insert to Data.
*   Implement `Search(vector)`: Find Centroids -> Scan Data -> Heap Sort.

## 8. Scalability & Optimization Process

To handle massive datasets (millions of vectors), the `Optimize` process (K-Means clustering and rebalancing) is designed for scalability and crash recovery.

### Batched Transactions
Instead of running the entire optimization in a single massive transaction (which would time out or consume excessive RAM), the process is broken down into small batches:
1.  **Batch Size**: Commits changes every 200 items.
2.  **Effect**: Keeps memory usage low and transaction duration short.

### Operational Constraints (Read-Only Mode)
To maintain data consistency without complex locking schemes, the Vector Store enforces a **Read-Only** mode during optimization.
*   **Mechanism**: An in-memory lock (`sync.Map`) tracks which domains are currently optimizing.
*   **Impact**: `Upsert` and `Delete` operations will fail with a specific error if called while `Optimize` is running. `Query` (Read) operations remain available.
*   **Trade-off**: We trade write availability for unlimited scalability and implementation simplicity.

### Crash Recovery
Since the lock is in-memory, if the process crashes, the lock is automatically released.
*   **Cleanup**: On the next start (or next `Optimize` call), the system detects if a previous run failed by checking for temporary artifacts (`_lku`, `_vecs`, `_centroids` stores).
*   **Action**: It automatically deletes these stale stores before starting a fresh optimization run.

## 9. Deletion & Garbage Collection

To support high-throughput deletions without immediate expensive rebalancing, we employ a **Tombstone** strategy combined with a **Garbage Collection** phase during optimization.

### The "Ghost Data" Problem
In a standard B-Tree, deleting an item is straightforward. However, in our dual-tree system:
1.  **Vectors Tree**: Stores the index key `{CentroidID, Distance, ItemID}`.
2.  **Content Tree**: Stores the actual data.

If we simply remove the entry from the `Vectors` tree during a `Delete` operation, the `Optimize` process (which iterates over the `Vectors` tree to rebuild the index) will never see that item again. Consequently, the item remains "orphaned" in the `Content` tree forever, leading to storage leaks.

### The Solution: Tombstones
Instead of physically removing the entry from the `Vectors` tree immediately:
1.  **Soft Delete**: The `Delete(id)` operation marks the item as deleted in the `Content` tree (`Deleted=true`).
2.  **Tombstone**: It also updates the corresponding key in the `Vectors` tree, setting a flag `IsDeleted=true`.
    *   *Note*: Since the key structure changes, this is technically a Remove + Add operation in the B-Tree, but it preserves the "pointer" to the data.

### Garbage Collection (The Reaper)
The `Optimize()` process acts as the Garbage Collector:
1.  **Scan**: It iterates through the `Vectors` tree to gather items for re-clustering.
2.  **Detect**: When it encounters a key with `IsDeleted=true`, it knows this item is dead.
3.  **Reap**: It performs a **Physical Delete** on the `Content` tree, permanently removing the data blob.
4.  **Skip**: The dead item is excluded from the new index being built.

This ensures that storage is reclaimed efficiently in batches during the maintenance window, keeping the runtime `Delete` operation fast and transactional.

