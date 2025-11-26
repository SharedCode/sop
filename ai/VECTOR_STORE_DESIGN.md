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
*   **Purpose**: Maps `CentroidID` -> `CentroidVector`.
*   **Size**: Small (e.g., 1k - 10k items). Fits in RAM.
*   **Key**: `int` (Centroid ID).
*   **Value**: `[]float32` (The vector coordinates of the center).
*   **Usage**: Used during Ingestion (to find where to insert) and Search (to find which buckets to scan).

### B-Tree #2: The Library (Data)
*   **Purpose**: Stores the actual user data, grouped by cluster AND ordered by distance-to-center.
*   **Size**: Huge (Billions of items). Disk-based.
*   **Key**: `CentroidID` (Primary) + `DistanceToCentroid` (Secondary) + `ItemID` (Tertiary).
    *   *Optimization*: By including `DistanceToCentroid` in the key, we can perform Range Scans within the bucket.
    *   *Triangle Inequality*: If `Query` is distance $D$ from `Centroid`, we only need to scan items with distance $[D-\epsilon, D+\epsilon]$ from `Centroid`.
*   **Value**: `VectorItem` (The raw vector + metadata).

## 4. The "SOP Advantage": Transactional Vector Store (OLTP)
Most Vector DBs (FAISS, HNSW) are **OLAP** (Analytical):
*   Optimized for raw read speed.
*   **Weakness**: Updates/Deletes are slow or impossible without rebuilding. No ACID transactions.

Our Design is **OLTP** (Transactional):
*   **Built on SOP**: Inherits ACID properties.
*   **Insert**: Atomic.
*   **Delete**: Atomic (Standard B-Tree delete).
*   **Update**: Atomic (Delete from Old Cluster -> Insert to New Cluster).
*   **Rollback**: Fully supported.
*   **Use Case**: Critical business data (Medical Records, User Profiles) where data integrity matches search capability.

## 5. Search Algorithm
1.  **Query**: User sends `QueryVector`.
2.  **Coarse Search (Directory)**:
    *   Scan `Centroids B-Tree` (or RAM Cache).
    *   Find the top $N$ closest Centroids (e.g., closest 3 clusters).
    *   Calculate `QueryDistToCentroid` for each.
3.  **Fine Search (Library)**:
    *   For each chosen Centroid ID:
        *   Open `Data B-Tree`.
        *   **Range Scan**: Search for keys where `CentroidID` matches AND `DistanceToCentroid` is in range `[QueryDistToCentroid - epsilon, QueryDistToCentroid + epsilon]`.
        *   *Benefit*: Skips reading items that are in the correct cluster but "far away" on the other side of the ring.
        *   Load candidates into memory.
4.  **Ranking (In-Memory)**:
    *   Calculate exact distance (`QueryVector` vs `ItemVector`).
    *   Maintain a Max-Heap of the top $K$ results.
5. **Result**: Return top $K$ items.

## 6. Data Management & Addressing (The Two-Way Flow)
A critical challenge in Vector Stores is addressing items by ID when they are physically stored by Cluster/Distance. We solve this using a **Content Store** (B-Tree #3) as a bridge.

### B-Tree #3: The Content Store
*   **Key**: `ItemID` (e.g., "doc-101").
*   **Value**: `Metadata` (JSON containing `_centroid_id` and `_distance`).

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
