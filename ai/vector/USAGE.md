# Vector Store Usage Pattern & Architecture

## Core Philosophy
The `ai/vector` package is designed to be a **cluster-ready**, **transactional** vector database built on top of SOP (Store of Persisted data). It adheres to a "Shared-Nothing" architecture where multiple clients can operate on the same index concurrently without local coordination.

**Note**: In clustered mode, Redis is used for coordination and caching, but **NOT** for data storage. The primary data resides on the filesystem (or shared volume), ensuring durability and scalability beyond RAM limits.

## The "Story" of Usage

### 1. The Stateless Client
The `vector.Database` struct is a lightweight, stateless handle. It does not maintain persistent connections or local locks that coordinate global state. It simply knows *where* the data is (Storage Path) and *how* to connect (SOP Factory).

```go
// Open a handle to the DB (Cluster-safe)
db := database.NewDatabase(database.DatabaseOptions{
    DBType:      database.Standalone,
    StoragePath: "/mnt/shared/sop_data",
})

// Open an index (Domain)
idx := db.Open(context.Background(), "product_catalog")
```

### 2. Transactional Operations
Every operation (`Upsert`, `Search`, `Delete`) is wrapped in a **SOP Transaction**. This ensures ACID properties across the multiple B-Trees that make up the Vector Index.

-   **Atomicity**: An `Upsert` that updates the `Content`, `Vectors`, and `Centroids` stores will either succeed completely or fail completely.
-   **Isolation**: Readers see a consistent snapshot. They do not see partial updates.
-   **Concurrency**: SOP handles conflict resolution. If two clients update the same item, one will retry or fail (depending on configuration), but the index will never be corrupted.

### 3. The "Active Version" Pattern (A/B Swapping)
To support heavy maintenance tasks like **Optimization** (K-Means retraining) without blocking readers or writers, we use a versioning system stored in SOP itself.

-   **State**: The `sys_config` B-Tree stores the `active_version` (e.g., `1764209652794618000` - Unix Nano).
-   **Readers**: Read `active_version` -> Open `vectors_{version}` -> Search.
-   **Writers (Upsert)**: Read `active_version` -> Open `vectors_{version}` -> Write.
-   **Optimize**:
    -   Uses K-Means clustering to partition vectors.
    -   Can be run periodically to re-cluster data as distribution changes.
    -   Uses a "Lookup + Sampling" strategy to scale to billions of records without loading all data into memory.

### 4. Cluster Safety
By removing local locks (like `sync.RWMutex`) and relying entirely on SOP's transactional integrity, we ensure that:
-   **Node A** can Upsert.
-   **Node B** can Optimize.
-   **Node C** can Search.
...simultaneously. If Node B swaps the version while Node A is writing, SOP's optimistic concurrency control will detect the conflict on the `active_version` or the underlying structures, ensuring data consistency.

## Data Structures (The 5-Table Layout)

1.  **Centroids Store**: `Btree[int, Centroid]`
    -   Fast lookup of cluster centers.
    -   `Centroid` contains `Vector` and `VectorCount`.
2.  **Vectors Store**: `Btree[CompositeKey, Vector]`
    -   The core IVF index. Key includes CentroidID and Distance for scanning.
3.  **Content Store**: `Btree[ItemID, JSON]`
    -   The source of truth. Shared across versions.
4.  **Lookup Store**: `Btree[int, ItemID]`
    -   Dense integer sequence for random sampling.
5.  **TempVectors Store**: `Btree[ItemID, Vector]`
    -   Temporary storage for vectors during the build phase.

## Intended Workflow

1.  **Ingest**: Use `UpsertBatch` for high throughput. You can configure `EnableIngestionBuffer` to `true` in `vector.Config` to use a buffered ingestion flow (Stage 0 -> Stage 1).
2.  **Query**: Use `Search` for low-latency retrieval.
3.  **Maintain**: Periodically call `Optimize` to optimize the index as data drifts, if there are changes. Since this performs a full index rebuild, schedule this based on your data volume and churn rate (e.g., nightly for moderate datasets, weekly for massive ones).

### Smart Optimization with `DynamicWithVectorCountTracking`

For applications that need more granular control over maintenance windows, use the `DynamicWithVectorCountTracking` mode.

-   **What it does**: In this mode, the system maintains real-time counters of how many vectors are assigned to each centroid.
-   **The Benefit**: Your application can inspect these counters to calculate the "skew" or density gap between clusters.
-   **Decision Logic**: Instead of a blind cron job, you can write logic like: *"If the largest cluster has 5x more vectors than the smallest, trigger Optimize."* This ensures you only pay the cost of a rebuild when the index structure has actually degraded enough to impact search performance.

### Explicit Centroid Management

Alternatively, applications can bypass the automatic clustering entirely and manage Centroids explicitly.

-   **How**: When calling `Upsert`, you can provide a specific `CentroidID` (default is 0, which means "auto-assign").
-   **Use Case**: If your application already has a logical grouping for vectors (e.g., "User ID", "Category ID", or pre-computed clusters), you can use that as the Centroid ID.
-   **Benefit**: This eliminates the need for `Optimize` calls, as the application logic ensures vectors are slotted correctly from the start. The Vector Store simply acts as a partitioned index respecting your grouping.
