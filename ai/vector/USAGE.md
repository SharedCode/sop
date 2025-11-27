# Vector Store Usage Pattern & Architecture

## Core Philosophy
The `ai/vector` package is designed to be a **cluster-ready**, **transactional** vector database built on top of SOP (Store of Persisted data). It adheres to a "Shared-Nothing" architecture where multiple clients can operate on the same index concurrently without local coordination.

## The "Story" of Usage

### 1. The Stateless Client
The `vector.Database` struct is a lightweight, stateless handle. It does not maintain persistent connections or local locks that coordinate global state. It simply knows *where* the data is (Storage Path) and *how* to connect (SOP Factory).

```go
// Open a handle to the DB (Cluster-safe)
db := vector.NewDatabase()
db.SetStoragePath("/mnt/shared/sop_data")

// Open an index (Domain)
idx := db.Open("product_catalog")
```

### 2. Transactional Operations
Every operation (`Upsert`, `Search`, `Delete`) is wrapped in a **SOP Transaction**. This ensures ACID properties across the multiple B-Trees that make up the Vector Index.

-   **Atomicity**: An `Upsert` that updates the `Content`, `Vectors`, and `Centroids` stores will either succeed completely or fail completely.
-   **Isolation**: Readers see a consistent snapshot. They do not see partial updates.
-   **Concurrency**: SOP handles conflict resolution. If two clients update the same item, one will retry or fail (depending on configuration), but the index will never be corrupted.

### 3. The "Active Version" Pattern (A/B Swapping)
To support heavy maintenance tasks like **Rebalancing** (K-Means retraining) without blocking readers or writers, we use a versioning system stored in SOP itself.

-   **State**: The `sys_config` B-Tree stores the `active_version` (e.g., `1764209652794618000` - Unix Nano).
-   **Readers**: Read `active_version` -> Open `vectors_{version}` -> Search.
-   **Writers (Upsert)**: Read `active_version` -> Open `vectors_{version}` -> Write.
-   **Rebalance**:
    1.  Read `active_version`.
    2.  Create `vectors_{new_version}` (Shadow Index).
    3.  Populate `vectors_{new_version}` from data.
    4.  **Atomic Swap**: Update `active_version` to `{new_version}` in a transaction.
    5.  Commit.

### 4. Cluster Safety
By removing local locks (like `sync.RWMutex`) and relying entirely on SOP's transactional integrity, we ensure that:
-   **Node A** can Upsert.
-   **Node B** can Rebalance.
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

1.  **Ingest**: Use `UpsertBatch` or `UpsertContent` + `IndexAll` for high throughput.
2.  **Query**: Use `Search` for low-latency retrieval.
3.  **Maintain**: Periodically call `Rebalance` to optimize the index as data drifts.
