# Operational Guide (DevOps)

This guide covers the operational aspects of running SOP in production, including failover handling, connection management, and backup strategies.

## Connection Management

SOP relies on long-lived (pooled) connections to Redis and Cassandra (for `incfs`).

> **Note**: If running in **Standalone Mode** (using `sop.InMemory` cache), Redis is not required, and this section can be ignored.

> **Note**: SOP does **not** require Redis data persistence (RDB/AOF). Redis is used for ephemeral locking and caching. If Redis restarts, SOP detects the change and recovers safely.

### Redis
*   **Pool**: SOP uses the standard `go-redis` client.
*   **Configuration**: Ensure your `redis.Options` includes:
    *   `PoolSize`: Set according to your concurrency needs (e.g., 10 * CPU cores).
    *   `MinIdleConns`: Keep some connections warm to avoid latency spikes.
    *   `ReadTimeout` / `WriteTimeout`: Tune these to avoid premature timeouts during heavy load.

### Cassandra (`incfs`)
*   **Consistency**: SOP typically uses `LOCAL_QUORUM` for strong consistency.
*   **Keyspace**: Ensure the keyspace is created with an appropriate replication strategy (e.g., `NetworkTopologyStrategy`) for your cluster.

## Failover Logic

SOP includes sophisticated logic to handle storage failures transparently.

### "Failover Qualified" I/O Errors

Not all errors trigger a failover. SOP distinguishes between transient errors (retryable) and permanent hardware/filesystem failures.

**Triggers for Failover:**
*   `syscall.EIO` (Input/output error)
*   `syscall.EROFS` (Read-only file system)
*   `syscall.ENOSPC` (No space left on device)
*   Specific Linux error codes for media failure (e.g., `EUCLEAN`).

**Behavior:**
1.  **Detection**: When a "Qualified" error occurs during a write.
2.  **Switch**: The system automatically marks the current storage path as "Passive" and switches to the configured "Active" standby path.
3.  **Recovery**: Operations continue on the new path. The failed path requires manual intervention or auto-repair (if configured).

### Monitoring

*   **Logs**: Watch for `[Failover]` tagged logs.
*   **Metrics**: Monitor the `sop_failover_count` metric (if you have instrumented your application).

## Backup & Restore

### Hybrid Backend (`incfs`)

Backing up a hybrid system requires coordination.

1.  **Snapshot Registry (Cassandra)**:
    *   Use `nodetool snapshot` to capture the state of the Cassandra keyspace.
2.  **Snapshot Blob Store (Filesystem)**:
    *   Use filesystem snapshots (e.g., ZFS, LVM, or cloud volume snapshots) to capture the data directory.
3.  **Consistency**:
    *   Ideally, pause writes during the snapshot window to ensure the Registry and Blob Store are perfectly aligned.
    *   If zero-downtime is required, snapshot Cassandra *first*, then the Filesystem. SOP's Copy-On-Write nature means old blobs (referenced by the older Cassandra snapshot) will still exist on disk, ensuring a consistent point-in-time restore.

### Restore Procedure

1.  Stop the application.
2.  Restore the Cassandra keyspace.
3.  Restore the Filesystem data.
4.  Start the application.
5.  **Run Integrity Check**: Use SOP's internal tools to verify that all Registry entries point to valid blobs.

## Data Management Suite (SOP Web UI)

SOP includes a powerful HTTP Server and Web UI that functions as a full database management suite. It allows you to:
*   **Perform Full CRUD**: Create, Read, Update, and Delete records in any B-Tree.
*   **Manage Any Database**: Works with any SOP store, regardless of the language or data type used.
*   **Leverage Rich Indexing**: Utilize `IndexSpecification` to define and search on compound indexes with multiple fields and custom sort orders, giving you RDBMS-like power.
*   **Debug & Inspect**: Visually verify data integrity and structure.

> **Note on Architecture**: This tool is **not** a central database server. In SOP's masterless architecture, this UI is simply another client node. You can run it locally on your laptop to manage a remote production cluster, or deploy it as a sidecar. It connects directly to the storage layer, respecting all ACID guarantees without introducing a central bottleneck. Each user managing data via this app participates in **"swarm" computing**, where changes are efficiently merged or rejected (if conflicting) with full ACID guarantees.

### Running the Management Suite

You can run the tool directly from the source:

```bash
# Point to your SOP registry folder
go run ./tools/httpserver -registry /path/to/your/sop/data
```

Access the UI at `http://localhost:8080`.

### Key Features
*   **Store Listing**: See all B-Trees in your registry.
*   **Rich Search**: Search by key prefix or complex multi-field queries (if using `IndexSpecification`).
*   **JSON Editor**: View and edit complex value objects as formatted JSON.
*   **Bulk Friendly**: Pagination and efficient loading for large datasets.
*   **Safe Schema Updates**: Admins can securely unlock and modify Index/CEL expressions on live stores.

For more details, see the [SOP Data Manager Documentation](tools/httpserver/README.md).
