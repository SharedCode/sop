# Configuration & Tuning Guide

This guide details the configuration options available in SOP and provides recommendations for tuning your stores for different workloads.

## StoreOptions Reference

The `StoreOptions` struct is the primary way to configure a B-Tree store.

| Field | Type | Description | Default / Recommendation |
| :--- | :--- | :--- | :--- |
| `Name` | `string` | Short name of the store. Must be unique within the repository. | Required. |
| `SlotLength` | `int` | Number of items stored in a single B-Tree node. | **Default: 1000**. **Max: 10,000**. Higher values (e.g., 5000) improve read performance but increase write latency. |
| `IsUnique` | `bool` | Enforces uniqueness of keys. | `true` for primary keys, `false` for non-unique indexes. |
| `IsValueDataInNodeSegment` | `bool` | Stores the Value directly inside the B-Tree node. | **Best for Small Data** (< 1KB). Improves locality. |
| `IsValueDataActivelyPersisted` | `bool` | Stores Value in a separate file/blob. | **Best for Big Data**. Prevents large values from bloating the B-Tree structure. |
| `IsValueDataGloballyCached` | `bool` | Caches the Value in Redis. | `true` for read-heavy workloads. `false` for write-heavy or very large data. |
| `LeafLoadBalancing` | `bool` | Checks siblings for space before splitting a node. | `false` (default). Set to `true` to save space at the cost of insert latency. |
| `BlobStoreBaseFolderPath` | `string` | Base path for the filesystem blob store. | Required for `inredfs` / `inredcfs`. |

## Performance Tuning

### Workload: Read-Heavy (e.g., User Profiles, Product Catalog)

*   **Goal**: Maximize cache hit rate and minimize I/O.
*   **Configuration**:
    *   `SlotLength`: **High** (e.g., 2000-5000). Fewer nodes to traverse.
    *   `IsValueDataGloballyCached`: **True**.
    *   `CacheConfig`: Increase `ValueDataCacheDuration` (e.g., 1 hour).

### Workload: Write-Heavy (e.g., Event Logging, IoT Telemetry)

*   **Goal**: Minimize locking contention and write amplification.
*   **Configuration**:
    *   `SlotLength`: **Moderate** (e.g., 500-1000). Reduces the cost of rewriting nodes during updates.
    *   `LeafLoadBalancing`: **False**. Avoids extra locking during inserts.
    *   `IsValueDataGloballyCached`: **False**. Don't pollute Redis with data that won't be read immediately.

### Workload: Large Objects (e.g., Images, Documents)

*   **Goal**: Keep the B-Tree lean.
*   **Configuration**:
    *   `IsValueDataInNodeSegment`: **False**.
    *   `IsValueDataActivelyPersisted`: **True**.
    *   Use the `streamingdata` package for objects > 1MB.

## Erasure Coding (EC)

SOP supports Erasure Coding for fault tolerance on the filesystem layer.

*   **Config**: `ErasureCodingConfig` (passed during Store creation).
*   **Recommendation**: Use for production deployments where disk failure is a concern.
*   **Overhead**: Adds CPU overhead for encoding/decoding but saves storage compared to full replication.

## Batch Size

When performing bulk operations (e.g., `UpdateMany`, `RemoveMany`), SOP processes items in batches.

*   **Default**: 500 items.
*   **Tuning**:
    *   Increase to **1000+** for high-throughput bulk loads.
    *   Decrease if you encounter transaction timeouts or memory pressure.

## Cache Configuration

SOP supports pluggable caching backends.

### Cache Factory

You can configure the global cache factory to switch between Redis (distributed) and In-Memory (standalone) modes.

```go
import "github.com/sharedcode/sop"

func init() {
    // Use Redis (Default) - Requires a running Redis server
    sop.SetCacheFactory(sop.Redis)

    // OR

    // Use In-Memory - No external dependencies
    sop.SetCacheFactory(sop.InMemory)
}
```

### Store Cache Config

The `StoreCacheConfig` struct controls how data is cached *within* the chosen backend.

| Field | Type | Description |
| :--- | :--- | :--- |
| `RegistryCacheDuration` | `time.Duration` | TTL for registry entries (Virtual ID -> Physical Location). |
| `StoreInfoCacheDuration` | `time.Duration` | TTL for store metadata. |
| `NodeCacheDuration` | `time.Duration` | TTL for B-Tree nodes. |
| `ValueDataCacheDuration` | `time.Duration` | TTL for value data (if stored separately). |
| `IsNodeCacheTTL` | `bool` | If `true`, accessing a node extends its cache TTL (Sliding Window). |
| `IsValueDataCacheTTL` | `bool` | If `true`, accessing a value extends its cache TTL. |

## Registry Partitioning & Tuning

SOP uses a "Registry" to map logical IDs (UUIDs) to physical file locations. This registry is partitioned into multiple "Segment Files" to manage file sizes and concurrency.

### Registry Hash Mod

The `RegistryHashModValue` determines the granularity of this partitioning. This value is configured **per database** (per `StoresBaseFolder`) via `TransactionOptions`.

*   **Flexibility**: You can configure different databases with different hash mod values based on their expected size.
    *   *Example*: Database A (User Profiles) uses `250` (Default) for efficiency.
    *   *Example*: Database B (IoT Logs) uses `1000` or `5000` to handle billions of records with fewer files.

*   **Formula**: `Segment File Size = RegistryHashModValue * 4096 bytes`
*   **Default**: `250` (Minimum).
    *   Segment Size: `250 * 4KB = 1MB` (approx).
    *   Capacity: **~10.7 Million items** per segment file (assuming `SlotLength` of 1000 and 65% node utilization).
        *   Calculation: `250 (Blocks) * 66 (Handles/Block) * 1000 (Items/Node) * 0.65 (Load) = 10,725,000`
*   **Maximum**: `750,000` (Creates ~3GB segment files).

**Scaling & File Handles**:
SOP automatically allocates additional segment files (e.g., `registry-1.reg`, `registry-2.reg`) as needed.
*   **Example**: Storing **1 Billion items** with the default hashmod (250) will result in approximately **100 segment files** (1B / 10.7M).
*   **Optimization**: For very large datasets, increasing `RegistryHashModValue` reduces the total file count, conserving OS file handles and simplifying backup operations.

### Alternative Optimization: Slot Length

Instead of increasing `RegistryHashModValue`, you can also optimize for large datasets by increasing the B-Tree `SlotLength`.

*   **Strategy**: Keep `RegistryHashModValue` at default (`250`) but increase `SlotLength` to `5000` (or up to the max of `10,000`).
*   **Effect**: Each B-Tree node becomes larger (acting like a "mini-table" of 5,000 items), which drastically increases the number of items managed per registry handle.
*   **Capacity Boost**:
    *   Calculation: `250 * 66 * 5000 * 0.65 = ~53.6 Million items` per segment file.
    *   **Result**: Storing **1 Billion items** would only require **~19 segment files** (vs 100 with `SlotLength` 1000).

### Tuning Guidelines

| Scenario | Recommendation | Rationale |
| :--- | :--- | :--- |
| **Small to Medium Datasets** (< 100M items) | **Default (250)** | Keeps segment files small (~1MB), minimizing I/O overhead for partial updates. |
| **Large Datasets** (> 1B items) | **Increase (e.g., 1000 - 5000)** | Creates larger segment files (4MB - 20MB). Reduces the total number of files on disk, which is better for filesystem performance and backup operations. |
| **High Concurrency** | **Moderate (500)** | Balances file size with lock contention (though SOP uses row-level locking, file handles are still a resource). |

**Note**: Changing `RegistryHashModValue` after a store has been created is **not supported** and will result in data inaccessibility. This value must be set once during the initial creation of the repository.

