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
