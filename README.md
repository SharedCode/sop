# Scalable Objects Persistence (SOP) Library

[![Discussions](https://img.shields.io/github/discussions/SharedCode/sop)](https://github.com/SharedCode/sop/discussions) [![CI](https://github.com/SharedCode/sop/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/SharedCode/sop/actions/workflows/go.yml) [![codecov](https://codecov.io/gh/SharedCode/sop/branch/master/graph/badge.svg)](https://app.codecov.io/github/SharedCode/sop) [![Go Reference](https://pkg.go.dev/badge/github.com/sharedcode/sop.svg)](https://pkg.go.dev/github.com/sharedcode/sop) [![Go Report Card](https://goreportcard.com/badge/github.com/sharedcode/sop)](https://goreportcard.com/report/github.com/sharedcode/sop)

Golang V2 code library for high-performance, ACID storage with B-tree indexing, Redis-backed caching, and optional erasure-coded replication.

## Table of contents

- Introduction
- Scalability & Limits
- High-level features and articles
- Architecture Guide
- Configuration & Tuning
- Operational Guide
- API Cookbook
- Quick start
- Lifecycle: failures, failover, reinstate, EC auto-repair
- Prerequisites
- Running integration tests (Docker)
 - Testing (unit, integration, stress)
- Usability
- SOP API discussions
- SOP for Python (sop4py)
- SOP AI Kit
- Community & support
- Contributing & license
 - Coordination model (OOA) and safety

## Cluster reboot procedure
When rebooting an entire cluster running applications that use SOP, follow this order to avoid stale locks and ensure clean recovery:

1) Gracefully stop all apps that use SOP across the cluster.
2) Stop the Redis service(s) used by these SOP apps.
3) Reboot hosts if needed (or proceed directly if not).
4) Start the Redis service(s) first and verify they are healthy.
5) Start the apps that use SOP.

Notes:
- SOP relies on Redis for coordination (locks, recovery bookkeeping). Bringing Redis up before SOP apps prevents unnecessary failovers or stale-lock handling during app startup.
- If any node was force-killed, SOP’s stale-lock and rollback paths will repair on next write; starting Redis first ensures that path has the needed state.

# Introduction
What is SOP?

Scalable Objects Persistence(SOP) is a raw storage engine that bakes together a set of storage related features & algorithms in order to provide the most efficient & reliable (ACID attributes of transactions) technique (known) of storage management and rich search, as it brings to the application, the raw muscle of "raw storage", direct IO communications w/ disk drives. In a code library form factor today.

SOP V2 ships as a Golang code library. Thus, it can be used for storage management by applications of many types across different hardware architectures & Operating Systems (OS), that are supported by the Golang compiler.

## Scalability & Limits
SOP is architected to handle **Petabyte-scale** datasets and **Trillions of objects**.
- **Capacity**: Up to 495 Billion items per segment (with 1,000 segments = 495 Trillion items).
- **Throughput**: Limited only by hardware (Redis Cluster + Storage I/O), not software.
- **Design**: Horizontal scaling via independent storage nodes and sharded registry.

See the full analysis in [Scalability & Limits](SCALABILITY.md).

## Key Use Cases

SOP is designed to be versatile, powering everything from small embedded tools to massive enterprise clusters.

### 1. Standalone App (Embedded DB)
*   **Scenario**: Desktop apps, CLI tools, or single-node services needing rich indexing.
*   **Why SOP**:
    *   **Pure Power**: Direct B-Tree indexing on disk.
    *   **Speed**: "NoCheck" transaction mode offers raw speed for read-heavy or single-writer workloads.
    *   **Simplicity**: No external database dependencies (just a local file structure).

### 2. Enterprise Cluster App
*   **Scenario**: Distributed systems requiring high availability and ACID guarantees.
*   **Why SOP**:
    *   **ACID Transactions**: Two-Phase Commit (2PC) across distributed nodes.
    *   **Scalability**: Cassandra-backed registry for infinite metadata scaling.
    *   **Resilience**: Active/Passive registry replication and Erasure Coding for data blobs ensure zero data loss.

### 3. AI Vector Database
*   **Scenario**: Storing and retrieving millions of vector embeddings for RAG (Retrieval-Augmented Generation) applications.
*   **Why SOP**:
    *   **Transactional & ACID**: Unlike eventual-consistency vector stores, SOP provides full ACID compliance for vector operations, ensuring no data loss or "ghost" vectors.
    *   **Novel Storage Schema**: Uses a composite key strategy (`CentroidID` + `DistanceToCentroid`) to map high-dimensional vectors onto standard B-Trees, enabling efficient range scans and transactional integrity.
    *   **Ideal Random Sampling**: Uses a novel "Lookup Tree" indexing algorithm to generate mathematically representative centroids, ensuring high-quality clustering even on sorted or skewed datasets.
    *   **Self-Healing Index**: Automatically optimizes clusters and tracks distribution in real-time, maintaining optimal search speeds as data grows to terabytes.
    *   **Flexible Deployment**: Run in **Standalone** mode (zero dependencies, in-memory cache) or **Clustered** mode (Redis-backed cache for distributed scale).

### 4. AI Agent with Local LLM (Ollama)
*   **Scenario**: Privacy-focused or cost-sensitive AI agents that need to "understand" user input before searching.
*   **Why SOP**:
    *   **Embedder Agent Pattern**: SOP supports a dual-agent architecture where a specialized "Nurse" agent (powered by a local LLM like Llama 3 via Ollama) translates vague user queries (e.g., "my tummy hurts") into precise clinical terms (e.g., "abdominal pain") before the main "Doctor" agent searches the vector database.
    *   **Zero Cost**: Run the entire stack (Vector DB + LLM + Application) on a single machine without any API fees.
    *   **Factory Reset Kit**: SOP's "Fallback Config" pattern allows you to ship a self-contained "restore disk" (JSON with raw data) that automatically rebuilds the binary B-Tree database if it's ever corrupted or deleted.

### 5. Blob Store (Media & Large Files)
*   **Scenario**: Storing and streaming massive files like 4K video, high-fidelity audio, or large datasets (1TB+).
*   **Why SOP**:
    *   **Streaming Data Store**: SOP's `StreamingDataStore` breaks large values into manageable chunks (e.g., 20MB) automatically.
    *   **Partial Updates**: You can update specific chunks of a large file (e.g., editing a video segment) without rewriting the entire file.
    *   **ACID Transactions**: Even for multi-gigabyte files, SOP guarantees transactional integrity. You can upload or update massive blobs in a transaction; if it fails, it rolls back cleanly.
    *   **Smart Resume**: Built-in support for seeking to specific chunks allows for "resume download" or "seek to timestamp" functionality out of the box.

### 6. AI Model Registry
*   **Scenario**: Managing versions of local AI models (weights, configurations) alongside the data they process.
*   **Why SOP**:
    *   **Unified Storage**: Store your models in the same ACID transaction as your training data and vector embeddings.
    *   **Versioning**: Built-in support for versioning models (e.g., "v1.0", "v1.1") using composite keys.

### 7. Embedded Search Engine
*   **Scenario**: Adding "Search this wiki" or "Filter by text" features to an application without managing a separate Elasticsearch cluster.
*   **Why SOP**:
    *   **Transactional Indexing**: Index documents in the same transaction as you save them. No "eventual consistency" lag.
    *   **BM25 Scoring**: Uses industry-standard ranking algorithms for relevance.
    *   **Zero Ops**: It's just a library. No separate process to manage or monitor.
*   **Why SOP**:
    *   **Unified Storage**: Store your training data (Vectors), metadata (Registry), and model artifacts (Blobs/JSON) in one ACID-compliant system.
    *   **Atomic Updates**: Update your model weights and the vector index they correspond to in a single transaction, preventing version mismatch.

## Core Innovations

### Rich Key Structures (Metadata-Carrying Keys)
SOP's B-Tree implementation supports complex structs as Keys, not just simple primitives. This allows applications to "ride" data in the Key itself, acting as a persistent metadata layer that is available during index traversal without fetching the Value.
*   **Benefit**: Critical state (like `Version`, `Deleted` flags, or `CentroidID` for vectors) is stored in the B-Tree node itself.
*   **Performance**: Operations like "List all non-deleted items" or "Find items in Centroid X" can be performed by scanning the Keys only, avoiding expensive I/O to fetch the full JSON payload (Value).
*   **Consistency**: This metadata participates in the same ACID transaction as the Value, ensuring the index structure and the data payload are always in sync.

For a deeper dive into the system's design and package structure (including the Public vs. Internal split), please see the [Architecture Guide](ARCHITECTURE.md).

For configuration options and performance tuning, see the [Configuration Guide](CONFIGURATION.md).

For operational best practices (failover, backups), see the [Operational Guide](OPERATIONS.md).

For code examples, check out the [API Cookbook](COOKBOOK.md).

See more details here that describe further, the different qualities & attributes/features of SOP, and why it is a good choice as a storage engine for your applications today: [Summary](README2.md)

Before I go, I would like to say, SOP is a green field, totally new. What is being shipped in V2 is just the start of this new product. We are barely scratching the surface of what can be done that will help storage management at super scale. SOP is a super computing enabler. The way its architecture was laid out, independent features and together, they are meant to give us the best/most efficient performance & IO of a group of computers (cluster), network & their storage, that can possibly give us.

## High level features/usability articles about SOP
See the entire list & details here: https://github.com/sharedcode/sop/blob/master/README2.md#high-level-features-articles-about-sop

## The Database Abstraction
SOP provides a high-level `database` package that simplifies configuration and management of your storage artifacts.

- **Unified Entry Point**: Manage B-Trees, Vector Stores, and Model Registries from a single `Database` instance.
- **Deployment Modes**:
    - **Standalone**: Uses in-memory caching and local storage. Ideal for single-node apps or development.
    - **Clustered**: Uses Redis for distributed caching and coordination. Ideal for production clusters.
- **Simplified Transactions**: `db.BeginTransaction` handles the complexity of configuring caching and replication for you.

## Quick start
SOP is a NoSQL-like key/value storage engine with built-in indexing and transactions. You only need Go to start (Redis is optional for distributed setups).

1) Plan your environment
- Ensure sufficient disk capacity for your datasets. SOP stores on local filesystems and can replicate across drives.

2) Prerequisites
- Go 1.24.3 or later (module requires go 1.24.3)
- (Optional) Redis (recent version) - required only for distributed/cluster mode or if using Redis-backed caching. **Note**: Redis is NOT used for data storage, just for coordination & to offer built-in caching.

3) Install and run Redis (Optional)
- If using distributed features, install Redis locally or point to your cluster.

4) Add SOP to your Go app
- Import package:
  - `github.com/sharedcode/sop/database` (Recommended: Unified entry point for B-Trees, Vector Stores, and AI Models)
  - `github.com/sharedcode/sop/ai` (AI Toolkit: Vector Database, Agents, and RAG)
  - `github.com/sharedcode/sop/infs` (Low-level: Direct access to filesystem-backed B-Trees)
- Repo path: https://github.com/sharedcode/sop

5) Initialize and start coding
- Use the `database` package to initialize your environment.
  ```go
  // Initialize (Standalone or Clustered)
  db := database.NewDatabase(database.Standalone, "/var/lib/sop")

  // Start a Transaction
  tx, _ := db.BeginTransaction(ctx, sop.ForWriting)

  // Open a Store (B-Tree, Vector, or Model)
  users, _ := db.NewBtree(ctx, "users", tx)

  // Perform Operations
  users.Add(ctx, "user1", "John Doe")

  // Commit
  tx.Commit(ctx)
  ```

6) Deploy
- Ship your app and SOP along your usual release flow (binary or container). If you expose SOP via a microservice, choose REST/gRPC as needed.

7) Permissions
- Ensure the process user has RW permissions on the target data directories/drives. SOP uses DirectIO and filesystem APIs with 4096-byte sector alignment.

Tip: Using Python? See “SOP for Python” below.

## Lifecycle: failures, failover, reinstate, EC auto-repair
SOP is designed to keep your app online through common storage failures.

- Blob store with EC: B-tree nodes and large blobs are stored using Erasure Coding (EC). Up to the configured parity, reads/writes continue even when some drives are offline. When failures exceed parity, writes roll back (no failover is generated) and reads may require recovery.
- Registry and StoreRepository: These metadata files use Active/Passive replication. Only I/O errors on Registry/StoreRepository can generate a failover. On a failover, SOP flips to the passive path and continues. When you restore the failed drive, reinstate it as the passive side; SOP will fast‑forward any missing deltas and return it to rotation.
- Auto‑repair: With EC repair enabled, after replacing a failed blob drive, SOP reconstructs missing shards automatically and restores full redundancy in the background.

See the detailed lifecycle guide (failures, observability, reinstate/fast‑forward, and drive replacement) in README2.md: https://github.com/SharedCode/sop/blob/master/README2.md#lifecycle-failures-failover-reinstate-and-ec-auto-repair

Also see Operational caveats: https://github.com/SharedCode/sop/blob/master/README2.md#operational-caveats

For planned maintenance, see Cluster reboot procedure: [Cluster reboot procedure](#cluster-reboot-procedure).

### Transaction idle maintenance (onIdle) & priority rollback sweeps
Each write or read transaction opportunistically invokes an internal onIdle() path after key phases. This lightweight pass performs two independent maintenance tasks:

1. Priority rollback sweeps: Recovers/rolls back interrupted higher-priority transactions by consulting per‑transaction priority log (.plg) files.
	- Cluster-wide coordination: This task is coordinated across the entire cluster (or all threads in standalone mode). Only one worker "wins" and performs the sweep at any given time, ensuring no redundant processing. This prevents unnecessary "swarm overload" on these onIdle services.
	- Restart fast path: On detecting a Redis (L2 cache) restart (run_id change) or on application start (in embedded mode), SOP triggers a one‑time sweep of all priority logs immediately, ignoring age. This accelerates recovery of any half‑committed writes that were waiting for the periodic window.
	- Periodic path: Absent a restart, one worker periodically processes aged logs. Base interval is 5 minutes. If the previous sweep found work, a shorter 2 minute backoff is used to drain backlog faster. Intervals are governed by two atomically updated globals: lastPriorityOnIdleTime (Unix ms) and priorityLogFound (0/1 flag).
	- Concurrency: A mutex plus atomic timestamp prevents overlapping sweeps; only one goroutine performs a rollback batch at a time even under high Begin() concurrency.
	- Rationale: Using onIdle piggybacks maintenance on natural transaction flow without a dedicated background goroutine, simplifying embedding into host applications that manage their own scheduling.

2. Expired transaction log cleanup: Removes obsolete commit/rollback artifacts.
	- Cluster-wide coordination: Like priority sweeps, this task is coordinated cluster-wide. Only one worker wins the right to perform the cleanup for a given interval (regular or accelerated).
	- If recent activity suggests potential pending cleanup (hourBeingProcessed != ""), a 5m cadence is used; otherwise a 4h cadence minimizes overhead during idle periods. Timing uses an atomic lastOnIdleRunTime.

Thread safety: Earlier versions used unsynchronized globals; these now use atomic loads/stores (sync/atomic) to eliminate race detector warnings when tests force timer rewinds. Tests that manipulate timing (to speed up sweep scenarios) reset the atomic counters instead of writing plain globals.

Operational impact: You generally do not need to call anything explicitly—just ensure transactions continue to flow. If you embed SOP in a service that may become read‑only idle for long stretches but you still want prompt rollback of higher‑priority interruptions, periodically issue a lightweight read transaction to trigger onIdle.

Testing notes: Unit tests rewind lastPriorityOnIdleTime and priorityLogFound (atomically) to force immediate sweep execution; this pattern is acceptable only in test code. Production code should never reset these values manually.

## Prerequisites
- Go 1.24.3+
- (Optional) Redis server (local or cluster) - for distributed coordination
- Data directories on disks you intend SOP to use (4096-byte sector size recommended)

## Running Integration Tests
You can run the SOP's integration tests from "infs" package using the following docker commands:
NOTE: you need docker desktop running in your host machine for this to work. Go to the sop root folder, e.g. ```cd ~/sop```, where sop is the folder where you cloned from github.
1. Build the docker image: ```docker build -t mydi .```
2. Run the docker image in a container: ```docker run mydi```
* Where "mydi" is the name of the docker image, you can use another name of your choice.

The docker image will be built with alpine (linux) and Redis server in it. Copy the SOP source codes to it. Setup target data folder and environment variable that tells the unit tests of the data folder path.
On docker run, the shell script ensures that the Redis server is up & running then run the ("infs" package's integration) test files.

You can pattern how the test sets the (datapath) env't variable so you can run the same integration tests in your host machine, if needed, and yes, you need Redis running locally for this to work.
See https://github.com/SharedCode/sop/blob/master/Dockerfile and https://github.com/SharedCode/sop/blob/master/docker-entrypoint.sh for more details.

If you’re using VS Code, there are ready-made tasks:
- Docker: Build and Test — builds image mydi
- Docker: Run Tests — runs tests in the container

## Testing (unit, integration, stress)
Run tests locally without Docker using build tags:

- Unit tests (fast): go test ./...
- Integration tests (require Redis running on localhost and a writable data folder):
	- Set environment variable datapath to your data directory (defaults to a local path if unset).
	- Run: go test -tags=integration ./infs/integrationtests
- Stress tests (long-running): go test -timeout 2h -tags=stress ./infs/stresstests/...

VS Code tasks provided:
- Go: Test (Unit)
- Go: Test (Integration)
- Go: Test (Stress)
- Go: Test (Unit + Integration) runs both in sequence

CI note: GitHub Actions runs unit tests on pushes/PRs; a nightly/manual job runs the stress suite with -tags=stress.

# Usability
See details here: https://github.com/sharedcode/sop/blob/master/README2.md#usability

# SOP API Discussions
See details here: https://github.com/sharedcode/sop/blob/master/README2.md#simple-usage

# SOP for Python (sop4py)
See details here: https://github.com/sharedcode/sop/tree/master/jsondb/python#readme
Check out the [Python Cookbook](jsondb/python/COOKBOOK.md) for code recipes.

# SOP for AI Kit
SOP includes a comprehensive AI toolkit for building local, privacy-first expert systems.
- **AI Documentation**: [ai/README.md](ai/README.md) - Overview of the AI module, Vector Store, and Agent framework.
- **AI Tutorial**: [ai/TUTORIAL.md](ai/TUTORIAL.md) - Step-by-step guide to building the "Doctor & Nurse" expert system.

## Timeouts and deadlines
SOP commits are governed by two bounds:
- The caller context (deadline/cancellation)
- The transaction maxTime (commit max duration)

The commit ends when the earlier of these two is reached. Internal lock TTLs use maxTime to ensure locks are bounded even if the caller cancels early.

Recommendation: If you want replication/log cleanup to complete under the same budget, set your context deadline to at least maxTime plus a small grace period.

## Reliability & Integrity
SOP implements a "Rock Solid" storage strategy ensuring data integrity and consistency across failures.

### Checksums (CRC32)
Every data block written to disk is protected by a CRC32 checksum.
- **Implementation**: `fs/marshaldata.go`
- **Mechanism**: The `marshalData` function appends a `crc32.ChecksumIEEE` to every block. `unmarshalData` validates this checksum on read, returning an error if data corruption (bit rot) is detected.
- **Zero-Copy Optimization**: Sparse (all-zero) blocks are optimized to skip checksum calculation while maintaining validity.

### Rollbacks (COW & Priority Logs)
SOP uses a robust rollback mechanism to recover from crashes or power failures during a transaction.
- **Implementation**: `fs/hashmap.cow.go`
- **Copy-On-Write (COW)**: Before modifying a registry sector, SOP creates a `.cow` backup file (`createCow`). If a crash occurs, the next accessor detects the COW file, verifies its integrity (using the embedded CRC32), and restores the original state (`restoreFromCow`).
- **Priority Logs**: Transaction logs (`.plg`) track in-flight transactions. The `onIdle` maintenance process scans these logs to identify and roll back abandoned or expired transactions, ensuring the system returns to a consistent state.

### Unified Locking (Cross-Platform)
SOP employs a "Redis-assisted, Storage-anchored" locking model that works consistently across operating systems (Linux, Windows, macOS).
- **Storage Anchors**: Exclusive access to storage sectors is enforced via claim markers on the disk itself, using standard filesystem APIs with 4096-byte sector alignment (DirectIO). This ensures that even if Redis (the coordination layer) is lost, the physical data remains protected by the filesystem's atomic guarantees.
- **Redis Coordination**: Redis is used for high-speed, ephemeral locking to reduce contention.
- **Cross-Platform Consistency**: By relying on standard file I/O and sector alignment rather than OS-specific locking primitives (like `flock` vs `LockFile`), SOP guarantees identical locking behavior on all supported platforms.

## Coordination model (OOA) and safety

### Coordination model: Redis-assisted, storage-anchored

SOP uses Redis for fast, ephemeral coordination and the filesystem for durable sector claims. Redis locks provide low-latency contention detection; per-sector claim markers on storage enforce exclusive access for CUD operations. This hybrid keeps coordination responsive without coupling correctness to Redis durability.

### Why this is safe (despite Redis tail loss/failover)

- Locks are advisory; correctness is anchored in storage-sector claims and idempotent commit/rollback.
- On Redis restart, SOP detects it and performs cleanup sweeps (clearing stale sector claims) before resuming.
- Time-bounded lock TTLs, takeover checks, and rollback paths ensure progress without split-brain.
- Priority logs and deterministic rollback let workers resume or repair safely after interruptions.

### Operational properties

- Decentralized: no leader or quorum; any node can coordinate on a sector independently.
- Horizontally scalable: sharded by registry sectors; no global hot spots.
- No single point of failure: loss of Redis state slows coordination briefly but doesn't corrupt data.
- Low latency: lock checks and claim writes are O(1) on hot path; no multi-round consensus.

### When Redis is unavailable

- Writes that need exclusivity will wait/fail fast; storage remains consistent.
- On recovery, restart sweeps clear stale sector claims; workers resume.

### Comparison to Paxos-style consensus

- SOP avoids global consensus, leader election, and replicated logs—lower coordination latency and cost.
- Better horizontal scaling for partitioned workloads (per-sector independence).
- No SPOF in the coordination layer; failover is trivial and stateless.
- If you need a globally ordered, cross-region commit log, consensus is still the right tool; SOP targets high-throughput, partition-aligned coordination. But then again, SOP is not a coordination engine, it is a storage engine. Its internal piece for coordination, e.g. - of handle (virtual ID) Registry, is what was described here.

### TL;DR

SOP builds a fast, decentralized coordination layer using Redis only for ephemeral locks and relies on storage-anchored sector claims for correctness. It scales out naturally and avoids consensus overhead while remaining safe under failover.

## Community & support
- Issues: https://github.com/SharedCode/sop/issues
- Discussions: https://github.com/SharedCode/sop/discussions (design/usage topics)

## Contributing & license
- Contributing guide: see CONTRIBUTING.md
- Code of Conduct: see CODE_OF_CONDUCT.md
- License: MIT, see LICENSE