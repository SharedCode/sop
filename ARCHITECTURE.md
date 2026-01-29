# SOP Architecture Guide

This document outlines the high-level architecture of the Scalable Objects Persistence (SOP) library, focusing on the package structure and the design decisions behind public vs. internal components.

## Package Structure & Visibility

SOP follows a strict separation between public APIs and internal implementation details to ensure a stable and safe developer experience.

### Project Structure

Here is an overview of the project's folder structure to help you navigate the codebase:

```text
sop/
├── adapters/          # Interface adapters (Redis, Cassandra)
├── ai/                # AI/Vector database modules & Python bindings
├── bindings/          # Cross-language bindings (Python)
├── btree/             # Core B-Tree data structure implementation
├── cache/             # Caching interfaces and implementations
├── database/          # High-level Database API (Entry point)
├── fs/                # Filesystem registry & I/O
├── incfs/             # Hybrid backend (Cassandra + Filesystem)
├── infs/              # Standard backend (Filesystem only)
├── inmemory/          # In-memory backend for standalone mode
├── internal/          # Internal implementation details (hidden)
├── jsondb/            # JSON document store
├── restapi/           # REST API server example
├── search/            # Search engine implementation (BM25)
└── streamingdata/     # Large object (BLOB) streaming support
```

### Public Packages

These packages are intended for direct use by consumers of the library:

*   **`github.com/sharedcode/sop/infs`**: The **primary and recommended** backend. It uses the local filesystem for both metadata (via a high-performance hashmap) and data. Redis is used strictly for **caching and coordination** (locking), not for data persistence.
    *   *Usage*: Ideal for both **distributed clusters** and single-node deployments. It outperforms the hybrid backend in stress tests.

*   **`github.com/sharedcode/sop/incfs`**: The "Hybrid" backend. It combines:
    *   **Cassandra**: For metadata and registry storage.
    *   **Filesystem**: For raw data storage.
    *   **Redis**: For caching and coordination.
    *   *Usage*: Use this if you have a specific requirement for Cassandra-based metadata management.

### Internal Packages

*   **`github.com/sharedcode/sop/internal/inredck`**:
    *   **Role**: This package contains the core implementation logic for the Redis/Cassandra interaction, specifically the "Cassandra Blob" pattern.
    *   **Why Internal?**: The pattern of storing B-Tree nodes as blobs in Cassandra is a powerful but complex implementation detail. Exposing this directly would risk users creating dependencies on internal storage formats or misusing the low-level blob management APIs. By keeping it internal, we enforce the use of the safe, transactional APIs provided by `incfs` and `streamingdata`.

## Design Principles

1.  **Encapsulation**: Complex storage logic (like the `inredck` blob management) is hidden behind clean, high-level interfaces (`incfs`).
2.  **ACID Transactions**: All public operations are designed to participate in SOP's Two-Phase Commit (2PC) transaction model.
3.  **Pluggable Backends**: The architecture supports different backend implementations (`infs` vs `incfs`) sharing common interfaces where possible.
4.  **UI-Driven Configuration**: Advanced store configuration, specifically **CEL Expressions** for custom sorting, is managed exclusively via the **Data Admin UI**. Language bindings (Go, Python, etc.) do not expose APIs for setting these expressions in code. This ensures that complex, dynamic logic is centralized in the administrative layer rather than scattered across application code.

## Development Guidelines

*   **Modifying Internal Packages**: If you are contributing to `internal/inredck`, be aware that changes here can affect both `incfs` and `streamingdata`. Always run the full integration test suite (`SOP_RUN_INCFS_IT=1`) after modifications.
*   **New Features**: New public features should generally be exposed via `incfs` or `streamingdata`, delegating to internal packages for the heavy lifting.

## Consistency & Caching Architecture

SOP employs a multi-tiered caching strategy designed to balance high performance with strict consistency, preventing stale reads even in high-throughput scenarios.

### The Stale Read Challenge

In a distributed system with local caching (L1), a common risk is **Local Staleness**:
1.  Transaction A on Host 1 updates Node X.
2.  Transaction B on Host 2 reads Node X from its local L1 cache.
3.  Host 2's L1 cache might still hold the old version of Node X, leading to a "Stale Read."

### The SOP Solution: Indirect Synchronization (The "Pheromone" Approach)

SOP solves this by utilizing a technique inspired by **Swarm Intelligence "Pheromone" algorithms**. Instead of synchronizing the bulky data (L1 Cache) across the swarm, we only synchronize the tiny navigational signals (Registry Handles).

1.  **Registry as Authority**: The Registry maps a Virtual ID (UUID) to a Physical Handle (Version + Physical ID). This mapping resides in the **L2 Cache (Redis)** or the persistent Registry file.
2.  **The "Check-First" Flow**:
    *   **Step 1**: When a transaction requests a Node by its Virtual ID, it **always** queries the Registry (L2) first.
    *   **Step 2**: The Registry returns the *current* Physical ID (e.g., `NodeID_v2`).
    *   **Step 3**: The transaction *then* checks the L1 cache for `NodeID_v2`.
    *   **Step 4**:
        *   **Hit**: If `NodeID_v2` is in L1, it is returned (Fast).
        *   **Miss**: If not, it is fetched from the Blob Store.
3.  **Why Staleness is Impossible**:
    *   If Host 1 updates Node X to `v2`, the Registry is updated to point Node X -> `NodeID_v2`.
    *   Host 2's L1 cache might still have `NodeID_v1`.
    *   When Host 2 requests Node X, the Registry tells it: "The current node is `NodeID_v2`."
    *   Host 2 looks for `NodeID_v2` in L1. It won't find it (or will find the correct new data). It will *never* accidentally return `NodeID_v1` because the Registry handle didn't ask for it.

> **Swarm Architecture Benefit**: This design eliminates the need for stressful messaging, broadcast invalidations, or heavy L1-L2 synchronization protocols. We intentionally **do not cache Registry Handles in L1**. By forcing this "tiny" indirect synchronization via the Registry (acting like a minimal pheromone trail), SOP allows the swarm to operate in a lightweight manner without the overhead of heavy cache coherence traffic.

### ACID Enforcement: The "Theory of Relativity" Approach

While the "Pheromone" synchronization ensures access to fresh data, SOP guarantees strict **ACID** compliance through a rigorous **Two-Points-in-Time** validation mechanism during the commit phase, effectively enforcing a "Theory of Relativity" for transactions.

Most distributed systems compromise on consistency to achieve speed, settling for "Eventual Consistency" (loosey-goosey state). SOP refuses this compromise.

1.  **Point A (Read Time)**: When the transaction acts (reads/writes), it captures the specific **Version** of every artifact involved.
2.  **Point B (Commit Time)**: During **Phase 2 Commit**, the system re-verifies these versions against the Registry (Source of Truth).

If the version at Point B matches the version at Point A, it proves that **"Time Stood Still"** for that transaction relative to the data it touched. No other actor interfered with the state. This mechanism guarantees **Snapshot Isolation** and strictly serializable behavior without the heavy locking overhead of traditional relational databases.

**The Result**: State-of-the-Art performance with enterprise-grade ACID guarantees. SOP delivers the speed of "Eventual Consistency" systems while strictly enforcing the correctness of a traditional RDBMS.

## Component Interaction & Backends

SOP supports two primary backends, each with a distinct architecture for handling metadata and data.

### 1. Filesystem Backend (`infs`) - **Recommended**

Designed for **distributed, high-scale environments** as well as single-node deployments.

*   **Structure**:
    *   The SOP database resides in the file system.
    *   Each **storage path** parameter provided during initialization corresponds to a **database folder**.
    *   Users can manage **many stores** (B-Trees) within each database folder.
    *   You can have **many database folders**, allowing you to create a vast set of databases, each dedicated to a specific taxonomy or domain.
*   **Registry (Filesystem)**: Stores metadata and the registry in a specialized, memory-mapped hashmap file on disk.
    *   *Why*: **Superior Performance**. The proprietary registry hashmap on disk, combined with Redis coordination, has been proven to scale better than the Hybrid Cassandra model. In stress tests simulating heavy workloads across machines on commodity hardware, `infs` performed **25% faster** than `incfs`.
    *   *Partitioning*: The registry is split into "Segment Files" (default 1MB) to manage concurrency and file sizes. See [Configuration Guide](CONFIGURATION.md#registry-partitioning--tuning) for tuning details.
*   **Blob Store (Filesystem)**: Stores nodes/values as blobs.
*   **Locking & Caching**:
    *   **Redis (Default)**: Uses Redis for distributed locking and caching.
    *   **In-Memory (Standalone)**: Can be configured to use internal memory for locking and caching, removing the Redis dependency entirely.

### 2. Hybrid Backend (`incfs`)

An alternative backend for distributed environments that **"Powers up"** your existing Cassandra infrastructure.

*   **Structure**:
    *   The SOP database metadata resides in Cassandra tables.
    *   Each **Keyspace** provided during initialization corresponds to a **logical database**.
    *   Users can manage **many stores** (B-Trees) within each Keyspace.
    *   You can have **many Keyspaces**, allowing you to create a vast set of databases, each dedicated to a specific taxonomy or domain.
*   **Registry (Cassandra)**: Stores metadata, B-Tree root information, and the "Virtual ID" registry.
    *   *Why*: **"Power up"** for Cassandra. This backend layers SOP's **ACID transactions** and **B-Tree indexing** (ordered data, range queries) on top of Cassandra, giving you the best of both worlds: Cassandra's robust replication for metadata and SOP's transactional consistency.
*   **Blob Store (Filesystem)**: Stores the actual B-Tree nodes and data values as serialized blobs.
    *   *Why*: Direct filesystem I/O is extremely fast and cost-effective for bulk data.
*   **Locking & Caching (Redis)**: Handles distributed locking and caches frequently accessed nodes.

### Transaction Data Flow

The flow of data during a **Commit** operation is similar for both backends, but the **Commit Point**—the moment the transaction becomes durable—differs.

#### `inredcfs` (Hybrid) Flow

```mermaid
sequenceDiagram
    participant App
    participant SOP as SOP Transaction
    participant Redis as Redis (Lock/Cache)
    participant FS as Blob Store (FS)
    participant Cass as Registry (Cassandra)

    App->>SOP: Commit()
    
    rect rgb(240, 248, 255)
        note right of SOP: Phase 1: Prepare
        SOP->>Redis: Acquire Locks (Rows/Items)
        SOP->>SOP: Conflict Detection
        SOP->>FS: Write "Dirty" Nodes (WAL/Temp)
    end

    rect rgb(255, 250, 240)
        note right of SOP: Phase 2: Commit
        SOP->>Cass: Update Registry (Virtual ID -> New Physical Location)
        SOP->>Redis: Update Cache (New Nodes)
        SOP->>Redis: Release Locks
    end

    rect rgb(240, 255, 240)
        note right of SOP: Cleanup (Async)
        SOP->>FS: Delete Old/Obsolete Nodes
    end

    SOP-->>App: Success
```

#### `infs` (Filesystem) Flow

The flow is identical to the above, except **Cassandra** is replaced by the **Filesystem Registry**.

1.  **Prepare**: Nodes are written to the Blob Store.
2.  **Commit**: The Registry file on disk is atomically updated (via `fsync`) to point to the new node locations.
3.  **Cleanup**: Old blobs are removed.

### Key Concepts

*   **Virtual IDs**: Every item and node in SOP is identified by a UUID. The Registry maps this UUID to its current physical location (e.g., a file path or blob ID). This allows us to move data (Copy-On-Write) without breaking references.
*   **Metadata-Carrying Keys**: SOP allows complex structs to be used as B-Tree keys. This enables the Key to act as a "Covering Index," storing essential state (like `Version`, `Deleted` flags, or `CentroidID`) directly in the B-Tree node. This allows structural operations to be performed by scanning Keys only, avoiding the I/O cost of fetching large Data Blobs (Values).
*   **Two-Phase Commit & The "Commit Point"**:
    *   **`inredcfs`**: The commit point is the atomic update of the Registry in **Cassandra**. Once the registry row is updated to point to the new blob location, the transaction is durable.
    *   **`infs`**: The commit point is the atomic update of the Registry hashmap on the **Filesystem**.
*   **Registry as the Source of Truth**: The Registry is the primary provider and assurer of ACID properties (Isolation, Atomicity).
    *   **Temporary Artifacts**: B-Tree nodes and data pages modified in a transaction are considered **temporary** until their handles are fully written to the Registry during the commit phase.
    *   **Lazy Cleanup**: Because these artifacts are not "live" until registered, their cleanup (garbage collection) can be performed at a "luxury of time" pace (defaulting to a 4-hour interval), reducing system overhead without compromising data integrity.

## Dual-View Architecture & Serialization Efficiency

SOP employs a unique **"Dual-View" Architecture** that decouples the storage format from the runtime representation, achieving high performance for both strongly-typed and dynamic use cases.

### 1. The "Common Ground": JSON on Disk
The underlying storage format for B-Tree nodes and items is **JSON** (via `DefaultMarshaler`). This neutral format on disk allows data to be agnostic of the consuming application's type system.

### 2. Direct Deserialization (Zero Waste)
SOP avoids the common "double conversion" penalty found in many ORMs or hybrid systems.
*   **The Inefficient Way**: `Disk Bytes` -> `Generic Map` -> `Strong Struct` (Double allocation/conversion).
*   **The SOP Way**:
    *   **App View (Generics)**: `Disk Bytes` -> `Strong Struct` (Direct `json.Unmarshal`).
    *   **Data Manager View (Dynamic)**: `Disk Bytes` -> `map[string]any` (Direct `json.Unmarshal`).

The Data Manager reads the *exact same bytes* as the Application but requests a `map[string]any`. The JSON decoder constructs the map directly, bypassing the need for the original Struct definition. This allows administrative tools to view and manipulate data without needing the application's source code or type definitions.

### 3. `JsonDBMapKey`: The Intelligence Layer
While `map[string]any` provides flexibility, it lacks inherent ordering. `JsonDBMapKey` bridges this gap by injecting a **Proxy Comparer** into the B-Tree.
*   **Role**: It teaches the B-Tree how to sort dynamic maps based on **Index Specifications** and/or **CEL Expressions** (allowing fine-grained comparison logic to complement standard indexing).
*   **Result**: You get the flexibility of a NoSQL document store (schema-less, dynamic) with the performance and ordered capabilities of a B-Tree (Range Queries, Prefix Scans), all without sacrificing type safety in the main application.

## Deployment Modes

SOP is designed to run in two distinct modes, catering to different scale requirements.

### 1. Filesystem Backend (`infs`) - **Recommended**
*   **Backend**: `infs`.
*   **Architecture**: Multiple application nodes, shared storage (Network FS/S3). Redis is used for **coordination only**.
*   **Use Case**: Enterprise applications, high-availability services, distributed clusters.
*   **Pros**: **Highest performance** (25% faster than hybrid), horizontal scalability, fault tolerance, ACID guarantees.

### 2. Hybrid Mode (Distributed)
*   **Backend**: `incfs`.
*   **Architecture**: Multiple application nodes, shared storage (Cassandra + Network FS/S3).
*   **Use Case**: Environments with existing Cassandra infrastructure.
*   **Pros**: **"Powers up"** Cassandra with ACID transactions and B-Tree indexing. Horizontal scalability, fault tolerance.

### 3. Standalone Mode (Embedded)
*   **Backend**: `infs` (or `inmemory` for pure RAM).
*   **Architecture**: Single process, local storage.
*   **Use Case**: Desktop apps, CLI tools, local AI vector stores.
*   **Pros**: Zero external dependencies, maximum single-node performance.

## AI & Agent Architecture

SOP introduces a novel architecture for AI Agents, distinguishing itself from standard "RAG" or "Chatbot" implementations by leveraging the B-Tree as the central nervous system.

### 1. The "Powerhouse" B-Tree Memory
Unlike systems that rely on vector databases or flat text files for memory, SOP treats **Memory as a Database System**.
*   **Long-Term Memory (LTM)**: Stored in a dedicated **ACID-compliant B-Tree** (`llm_knowledge`). This ensures that the Agent's knowledge base is transactional, ordered, and scalable ($O(\log N)$ retrieval). The Agent can safely update its own mind (Self-Learning) without corruption.
*   **Short-Term Memory (STM)**: Implemented as **Structured Threads** (`ConversationThread`), not a flat list of tokens. This gives the Agent "Executive Function"—the ability to track topics, manage context switches, and maintain a rigorous "Train of Thought" separate from the raw chat history.

### 2. Hybrid Scripting Engine (Explicit Execution)
The SOP Scripting Engine (`ai/agent`) follows a unique **"Explicit Execution"** design pattern.
*   **No "Magic" Compilation**: The engine is "dumb and obedient." It does not try to guess user intent or "compile away" interaction steps.
*   **Hybrid Flow**: Scripts naturally mix **Deterministic Commands** (e.g., `scan`, `filter`) with **Probabilistic Reasoning** (e.g., `ask`).
*   **Run-Loop Scripting**: The Agent can pause a deterministic workflow to ask the LLM for guidance ("Analyze these results"), and then resume execution based on the LLM's structured response. This allows for essentially infinite complexity in agentic behaviors without the fragility of pure-LLM loops.

## Backend Comparison: Isolation & Concurrency

When choosing a backend, it is crucial to understand how they handle isolation, locking, and multi-tenancy. Both backends support high concurrency, but their locking scopes differ.

| Feature | **FileSystem (`infs`)** | **Cassandra (`incfs`)** |
| :--- | :--- | :--- |
| **Primary Use Case** | High-performance distributed or local clusters. | Environments with existing Cassandra infrastructure. |
| **Multi-Tenancy** | **Directory-Based**: Each database is a separate folder on disk. | **Keyspace-Based**: Each database is a separate Keyspace in Cassandra. |
| **Locking Scope** | **`BaseFolder:StoreName`**<br>Locks are isolated to the specific database folder. Two stores with the same name in different folders *do not* block each other. | **`Keyspace:StoreName`**<br>Locks are isolated to the specific Keyspace. Two stores with the same name in different keyspaces *do not* block each other. |
| **Concurrency** | **High**. Operations on different databases (folders) are completely independent. | **High**. Operations on different keyspaces are completely independent. |
| **Metadata Storage** | Custom high-performance Hash Map on disk. | Cassandra Tables (`store`, `registry`, etc.). |
| **Data Storage** | Filesystem Blobs. | Filesystem Blobs. |
| **Coordination** | Redis (Distributed) or In-Memory (Standalone). | Redis. |

### Isolation & Locking Details

SOP uses **Redis** (in distributed mode) to manage transaction locks. The key design principle is that **locking is scoped to the logical database**.

*   **FileSystem Backend**: The "Logical Database" is defined by the `StorePath` (the folder path).
    *   *Example*: If you have `db1/users` and `db2/users`, a transaction on `db1/users` acquires a lock on `db1:users`. It will **never** block a transaction on `db2/users`.
*   **Cassandra Backend**: The "Logical Database" is defined by the `Keyspace`.
    *   *Example*: If you have `keyspaceA.users` and `keyspaceB.users`, a transaction on `keyspaceA` acquires a lock on `keyspaceA:users`. It will **never** block `keyspaceB`.

This architecture ensures that SOP can host thousands of independent databases (tenants) on the same infrastructure without lock contention between them.

## Reliability & Self-Healing

SOP incorporates advanced mechanisms to ensure data integrity and system stability, particularly in distributed environments where infrastructure components like Redis may restart or fail.

### Redis Restart Detection (Clustered Mode)

In Clustered mode, SOP relies on Redis for transaction locking and coordination. A Redis restart could potentially lose volatile lock information, leaving transactions in an indeterminate state. To mitigate this, SOP implements a **"Not Restarted" Token** mechanism:

1.  **The Token**: A special volatile key (`notrestarted`) is maintained in Redis with a sliding expiration (TTL).
2.  **Detection**: The background servicer (`onIdle`) periodically checks for this token.
    *   **Presence**: If the token exists, Redis is stable.
    *   **Absence**: If the token is missing (e.g., after a restart), the system infers a potential restart event.
3.  **Action**: Upon detecting a restart, the system triggers a **Lock Resurrection** process. It scans for incomplete transactions (via Priority Logs) and re-acquires the necessary locks to allow those transactions to either complete or roll back safely.

### Transaction Lifecycle Management

*   **Clustered Mode**: The background servicer continuously monitors for "dead" transactions and Redis restarts, ensuring that locks are restored and incomplete transactions are resolved without corrupting the registry.
*   **Standalone Mode**: Since there is no external Redis dependency, the application performs a comprehensive **Priority Rollback Sweep** immediately upon startup. This cleans up any transactions that were interrupted by a previous application crash.

This multi-layered approach ensures that SOP databases remain "rock solid" and self-healing, minimizing the need for manual administrative intervention.

## Future Optimization Roadmap

### Registry Partitioning: Scalable Clusters & Binary Search
As SOP scales to handle **trillion to hundreds of trillions of items**, the current linear chaining of registry segment files (while effective and simple) presents an opportunity for optimization.

*   **Current State**: Sequential allocation (Chaining). New segment files are added one by one (`registry-1`, `registry-2`...) as needed. Lookup is linear $O(N)$.
*   **Proposed Future State**: Clustered Allocation & Binary Seach.
    *   **Clustering**: Instead of allocating files singly, the system could pre-allocate "Clusters" of segment files.
        *   **Tunable Size**: The cluster size (e.g., 10, 20, 30 files per cluster) would be configurable to match the deployment scale.
    *   **Binary Search**: With a known, structured set of files, the registry lookup algorithm can switch from a sequential scan to a **Binary Search** across the file segments to locate the correct virtual ID bucket.
*   **Impact**:
    *   **Extreme Scale**: Enables management of storage at the **hundreds of trillions of items** level.
    *   **Scalability**: Allows supporting thousands of segment files without the performance penalty of a linear scan.
    *   **Throughput**: Reduces the "Worst Case" lookup from $O(N)$ file headers to $O(\log N)$, significantly conserving IOPS for these hyper-scale deployments.


