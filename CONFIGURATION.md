# Configuration & Tuning Guide

This guide details the configuration options available in SOP and provides recommendations for tuning your stores for different workloads.

## StoreOptions Reference

The `StoreOptions` struct is the primary way to configure a B-Tree store.

| Field | Type | Description | Default / Recommendation |
| :--- | :--- | :--- | :--- |
| `Name` | `string` | Short name of the store. Must be unique within the repository. | Required. |
| `SlotLength` | `int` | Number of items stored in a single B-Tree node. | **Default: 1000**. Higher values (e.g., 5000) improve read performance but increase write latency. |
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
