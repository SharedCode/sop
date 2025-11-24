# Scalable Objects Persistence (SOP) Library

[![Discussions](https://img.shields.io/github/discussions/SharedCode/sop)](https://github.com/SharedCode/sop/discussions) [![CI](https://github.com/SharedCode/sop/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/SharedCode/sop/actions/workflows/go.yml) [![codecov](https://codecov.io/gh/SharedCode/sop/branch/master/graph/badge.svg)](https://app.codecov.io/github/SharedCode/sop) [![Go Reference](https://pkg.go.dev/badge/github.com/sharedcode/sop.svg)](https://pkg.go.dev/github.com/sharedcode/sop) [![Go Report Card](https://goreportcard.com/badge/github.com/sharedcode/sop)](https://goreportcard.com/report/github.com/sharedcode/sop)

Golang V2 code library for high-performance, ACID storage with B-tree indexing, Redis-backed caching, and optional erasure-coded replication.

## Table of contents

- Introduction
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
    *   **Optimal Storage**: SOP B-Trees are uniquely suited for vector storage. You can store high-dimensional vectors (blobs) directly in the B-Tree values. By configuring `IsValueDataInNodeSegment=false`, SOP keeps the index structure compact (keys only) while offloading the heavy vector data to separate data segments. This ensures high-performance traversals and efficient memory usage.
    *   **Partitioned Search**: SOP's architecture supports natural partitioning (e.g., by `PartitionID` + `VectorID`), enabling scalable, parallelized vector searches across massive datasets.
    *   **Hybrid Search**: Combine vector similarity with traditional B-Tree range queries (e.g., "Find vectors where date > 2024") in a single, ACID-compliant transaction.
    *   **Standalone Capable**: Run a full-featured vector store in your application with zero external dependencies (no Redis required) using SOP's in-memory caching and local filesystem storage.

For a deeper dive into the system's design and package structure (including the Public vs. Internal split), please see the [Architecture Guide](ARCHITECTURE.md).

For configuration options and performance tuning, see the [Configuration Guide](CONFIGURATION.md).

For operational best practices (failover, backups), see the [Operational Guide](OPERATIONS.md).

For code examples, check out the [API Cookbook](COOKBOOK.md).

See more details here that describe further, the different qualities & attributes/features of SOP, and why it is a good choice as a storage engine for your applications today: [Summary](README2.md)

Before I go, I would like to say, SOP is a green field, totally new. What is being shipped in V2 is just the start of this new product. We are barely scratching the surface of what can be done that will help storage management at super scale. SOP is a super computing enabler. The way its architecture was laid out, independent features and together, they are meant to give us the best/most efficient performance & IO of a group of computers (cluster), network & their storage, that can possibly give us.

## High level features/usability articles about SOP
See the entire list & details here: https://github.com/sharedcode/sop/blob/master/README2.md#high-level-features-articles-about-sop

## Quick start
SOP is a NoSQL-like key/value storage engine with built-in indexing and transactions. You only need Go to start (Redis is optional for distributed setups).

1) Plan your environment
- Ensure sufficient disk capacity for your datasets. SOP stores on local filesystems and can replicate across drives.

2) Prerequisites
- Go 1.24.3 or later (module requires go 1.24.3)
- (Optional) Redis (recent version) - required only for distributed/cluster mode or if using Redis-backed caching.

3) Install and run Redis (Optional)
- If using distributed features, install Redis locally or point to your cluster.

4) Add SOP to your Go app
- Import package:
  - `github.com/sharedcode/sop/inredfs` (Recommended: lean, storage on filesystem, supports both in-memory and Redis-backed caching)
  - `github.com/sharedcode/sop/inredcfs` (Hybrid: Cassandra for metadata/registry, Filesystem for data, Redis-backed caching)
- Repo path: https://github.com/sharedcode/sop

5) Initialize Redis and start coding
- Initialize Redis connection, open a transaction, create/open a B-tree, then use CRUD and search (FindOne, First/Last/Next/Previous, paging APIs). See API links below.

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
	- Restart fast path: On detecting a Redis (L2 cache) restart (run_id change), SOP triggers a one‑time sweep of all priority logs immediately, ignoring age. This accelerates recovery of any half‑committed writes that were waiting for the periodic window.
	- Periodic path: Absent a restart, one worker periodically processes aged logs. Base interval = (fs.LockFileRegionDuration + 2m). If the previous sweep found work, a shorter 30s backoff is used to drain backlog faster. Intervals are governed by two atomically updated globals: lastPriorityOnIdleTime (Unix ms) and priorityLogFound (0/1 flag).
	- Concurrency: A mutex plus atomic timestamp prevents overlapping sweeps; only one goroutine performs a rollback batch at a time even under high Begin() concurrency.
	- Rationale: Using onIdle piggybacks maintenance on natural transaction flow without a dedicated background goroutine, simplifying embedding into host applications that manage their own scheduling.

2. Expired transaction log cleanup: Removes obsolete commit/rollback artifacts. If recent activity suggests potential pending cleanup (hourBeingProcessed != ""), a 5m cadence is used; otherwise a 4h cadence minimizes overhead during idle periods. Timing uses an atomic lastOnIdleRunTime.

Thread safety: Earlier versions used unsynchronized globals; these now use atomic loads/stores (sync/atomic) to eliminate race detector warnings when tests force timer rewinds. Tests that manipulate timing (to speed up sweep scenarios) reset the atomic counters instead of writing plain globals.

Operational impact: You generally do not need to call anything explicitly—just ensure transactions continue to flow. If you embed SOP in a service that may become read‑only idle for long stretches but you still want prompt rollback of higher‑priority interruptions, periodically issue a lightweight read transaction to trigger onIdle.

Testing notes: Unit tests rewind lastPriorityOnIdleTime and priorityLogFound (atomically) to force immediate sweep execution; this pattern is acceptable only in test code. Production code should never reset these values manually.

## Prerequisites
- Go 1.24.3+
- (Optional) Redis server (local or cluster) - for distributed coordination
- Data directories on disks you intend SOP to use (4096-byte sector size recommended)

## Running Integration Tests
You can run the SOP's integration tests from "inredfs" package using the following docker commands:
NOTE: you need docker desktop running in your host machine for this to work. Go to the sop root folder, e.g. ```cd ~/sop```, where sop is the folder where you cloned from github.
1. Build the docker image: ```docker build -t mydi .```
2. Run the docker image in a container: ```docker run mydi```
* Where "mydi" is the name of the docker image, you can use another name of your choice.

The docker image will be built with alpine (linux) and Redis server in it. Copy the SOP source codes to it. Setup target data folder and environment variable that tells the unit tests of the data folder path.
On docker run, the shell script ensures that the Redis server is up & running then run the ("inredfs" package's integration) test files.

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
	- Run: go test -tags=integration ./inredfs/integrationtests
- Stress tests (long-running): go test -timeout 2h -tags=stress ./inredfs/stresstests/...

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

## Timeouts and deadlines
SOP commits are governed by two bounds:
- The caller context (deadline/cancellation)
- The transaction maxTime (commit max duration)

The commit ends when the earlier of these two is reached. Internal lock TTLs use maxTime to ensure locks are bounded even if the caller cancels early.

Recommendation: If you want replication/log cleanup to complete under the same budget, set your context deadline to at least maxTime plus a small grace period.

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