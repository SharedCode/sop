# SOP

## One engine for data and compute.

**SOP** (Scalable Objects Persistence) is an embedded B-Tree storage engine and distributed computing platform written in Go with bindings for Python and C#. It combines **transactional data persistence**, **ordered key-value storage**, **vector similarity search**, and **swarm task coordination** into one library. 

Instead of managing separate database servers, message brokers, caching tiers, and distributed lock managers, SOP lets your application store data and coordinate distributed work in the same programming model.

[![Discussions](https://img.shields.io/github/discussions/SharedCode/sop)](https://github.com/SharedCode/sop/discussions)
[![CI](https://github.com/SharedCode/sop/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/SharedCode/sop/actions/workflows/go.yml)
[![Release](https://img.shields.io/github/v/release/SharedCode/sop)](https://github.com/SharedCode/sop/releases)
[![codecov](https://codecov.io/gh/SharedCode/sop/branch/master/graph/badge.svg)](https://app.codecov.io/github/SharedCode/sop)
[![Go Reference](https://pkg.go.dev/badge/github.com/sharedcode/sop.svg)](https://pkg.go.dev/github.com/sharedcode/sop)
[![Go version](https://img.shields.io/github/go-mod/go-version/SharedCode/sop)](go.mod)
[![License](https://img.shields.io/github/license/SharedCode/sop)](LICENSE)
[![Live Demos](https://img.shields.io/badge/Live_Demos-GitHub_Pages-10B981?logo=github)](https://sharedcode.github.io/sop-arena/)

---

## 🚀 Experience SOP

You can test SOP directly in your browser without installing anything:

| Experience | Description | Live Interactive Link |
| :--- | :--- | :--- |
| 🧠 **SOP Technical Demo** | **Client-Side Zero-Server WebAssembly Engine**<br>Execute live ACID transactions, 128-dimensional vector cosine searches, and microsecond benchmarks running 100% in your browser with **0 HTTP network calls**. | [**Launch Technical Demo →**](https://sharedcode.github.io/sop/) |
| 🎮 **SOP Arena** | **Distributed Systems Survival Simulation**<br>Command a live digital cluster. Scale worker swarms, crash storage nodes, trigger transaction storms, and watch SOP automatically redistribute tasks and rebuild parity in real-time. | [**Play SOP Arena →**](https://sharedcode.github.io/sop-arena/) |

<p align="center">
  <img src="docs/assets/sop-demo.gif" alt="SOP Interactive Demos and Distributed Systems Architecture Showcase" width="800" />
</p>

---

## 💡 What Problem Does SOP Solve?

Most distributed applications require two fundamentally different operations:
1. **Storing state reliably** (databases, key-value stores, vector indexes)
2. **Coordinating work across machines** (task queues, locks, retries, worker failovers)

Today, developers solve this by assembling a multi-component infrastructure stack:

```
THE FRAGMENTED MULTI-COMPONENT STACK (Without SOP):

[ Application ]
       │
       ├──► (TCP Hop 1: 5-15ms)  ──► Redis (Distributed Locks & Leases)
       ├──► (TCP Hop 2: 5-15ms)  ──► RabbitMQ / Kafka (Task Queue)
       ├──► (TCP Hop 3: 10-30ms) ──► PostgreSQL / Cassandra (Persistent Storage)
       └──► (Failover Glue)      ──► ZooKeeper / Custom Retry & Outbox Daemons

⚠️ 6+ infrastructure boundaries | 15-50ms latency tax | High split-brain failure risk | High maintenance overhead
```

When an application worker crashes between releasing a lock in Redis and committing to PostgreSQL, state can enter an inconsistent split-brain condition. Engineering teams end up spending substantial time writing and maintaining outbox listeners, lock renewers, and compensating retry logic.

---

## ⚡ Why SOP?

SOP takes a different approach: **co-locate storage and compute inside the same engine boundary.**

```
THE UNIFIED DATA & COMPUTE PLATFORM (With SOP):

[ Application ]
       │
       └──► (Embedded In-Process Call: < 0.3ms latency)
            ┌─────────────────────────────────────────────────────────────┐
            │                         SOP ENGINE                          │
            │  • Persistent B-Tree Storage (Sector-aligned Direct I/O)    │
            │  • Strict Serializable ACID Transactions (WAL + 2PC)       │
            │  • Swarm Compute & Autonomous Task Redistribution           │
            │  • High-Dimensional Vector Similarity Indexing (SIMD)       │
            │  • Reed-Solomon Erasure Coding & Partition Resilience       │
            └─────────────────────────────────────────────────────────────┘

✓ 1 Single Engine | Sub-millisecond execution | 100% ACID consistency | Automated failover
```

Because compute workers, task queues, and storage partitions share the same transaction boundary, a worker failure triggers an automatic rollback of uncommitted work and re-assigns the task in milliseconds with zero orphan locks.

---

## ⏱️ Why Now?

Three industry shifts make this architecture increasingly relevant:

1. **The Explosion of Autonomous AI Agents**: Multi-agent swarms require frequent context checkpointing, vector similarity searches, and task coordination. Assembling this across Postgres, Pinecone, Redis, and Celery creates high failure surface area.
2. **Edge and Local-First Computing**: Devices in factory automation, vehicles, and retail branches cannot rely on constant connections to central cloud databases. They need full ACID storage and local coordination that works offline.
3. **Infrastructure Simplification**: Engineering organizations are seeking to reduce the operational overhead and cloud bills associated with running dozens of discrete microservices just to manage state and queues.

---

## 🔍 What Makes SOP Different?

SOP is built on five core technical principles:

1. **Embedded Storage Engine**: Operates in-process in Go, Python, and C#, eliminating TCP network hops for local reads and writes.
2. **ACID Transactions without Database Servers**: Implements Write-Ahead Logging (WAL) and Two-Phase Commit (2PC) with copy-on-write page isolation.
3. **Swarm Compute Coordination**: Workers coordinate task execution using storage-anchored sector claims and heartbeat leases without requiring global consensus bottlenecks (like Paxos or Raft) on the hot path.
4. **Reed-Solomon Erasure Coding**: Protects storage shards from hardware failure by striping parity blocks across drives rather than paying the 3x disk storage cost of full replication.
5. **Integrated Vector & Structured Storage**: Stores high-dimensional vector embeddings in the same B-Tree segments as structured metadata, allowing single-transaction memory commits.

---

## ⚖️ SOP vs. Alternatives

Every architecture involves tradeoffs. Here is an honest comparison of where SOP fits relative to industry standards:

| Capability | PostgreSQL | Redis | Kafka | Temporal | Pinecone | SQLite | SOP |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **ACID Transactions** | ✓ | △ | ✗ | ✗ | ✗ | ✓ | ✓ |
| **Ordered B-Tree Range Scans** | ✓ | △ | ✗ | ✗ | ✗ | ✓ | ✓ |
| **Embedded In-Process** | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | ✓ |
| **Swarm Work Coordination** | ✗ | △ | △ | ✓ | ✗ | ✗ | ✓ |
| **Vector Similarity Search** | △ (pgvector) | △ | ✗ | ✗ | ✓ | ✗ | ✓ |
| **Erasure Coding (N+K)** | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ |
| **Zero Standalone Daemons** | ✗ | ✗ | ✗ | ✗ | ✗ | ✓ | ✓ |

*Legend: `✓` First-class native capability | `△` Partial or requires plugin/extension | `✗` Not designed for this capability*

### Detailed Tradeoffs by Competitor:

- **PostgreSQL**: Industry standard for general relational databases. Choose Postgres when you need complex relational schemas, advanced SQL aggregations, or standard ecosystem tooling. SOP is better suited when you want an embedded storage engine inside your application process without database server management.
- **Redis**: Industry standard for ultra-low-latency in-memory key-value caching. Choose Redis when all data fits in RAM and you need simple cache operations. SOP provides durable B-Tree disk persistence, multi-account ACID transactions, and erasure coding.
- **Kafka / RabbitMQ**: Industry standards for high-volume streaming and pub/sub. Choose Kafka when you need multi-datacenter event streams and log retention. SOP provides transactional task queues co-located with storage state for local swarms.
- **Temporal**: Industry standard for long-running durable workflows spanning external microservices. Choose Temporal for multi-week human-in-the-loop workflows across disparate clouds. SOP is designed for local-to-cluster co-located data and task execution.
- **SQLite**: Industry standard for embedded single-file relational databases. Choose SQLite for client desktop/mobile apps needing SQL. SOP is designed for high-concurrency multi-threaded workers, clustered coordination, partitioned vector stores, and erasure coding.

---

## 🎯 When SOP is a Great Fit

- **AI Agent Memory & Swarm Workforces**: Autonomous agents requiring durable conversation memory, vector search, and task hand-offs without fragmented infrastructure.
- **Real-Time Systems & Simulation State**: Game servers, robotics, and spatial computing needing microsecond state serialization.
- **Financial & Escrow Ledgers**: Systems requiring strict serializability and invariant verification (e.g. zero-sum account delta) prior to commit.
- **Edge & IoT Computing**: Devices operating in intermittent network environments that need local ACID persistence and peer synchronization.
- **Serverless Workloads**: Cloud functions that need durable storage without exhausting database connection pools.

---

## 🚫 When SOP is NOT the Right Tool

To be completely clear on architectural boundaries:

- **Massive Analytical Warehousing**: If you are running multi-petabyte columnar analytics across billions of historical events, specialized OLAP warehouses (like ClickHouse or Snowflake) are the right choice.
- **Global Multi-Region Consensus**: If your application requires synchronous commits across continents with multi-region Raft/Paxos quorums, dedicated distributed SQL databases (like CockroachDB or Google Spanner) are designed for that problem.
- **Simple Stateless CRUD Apps**: If your application is a standard CRUD dashboard with low traffic, standard PostgreSQL or MySQL with an ORM is simpler and has more ecosystem plugins.

---

## 🎮 See SOP in Action (SOP Arena Simulation)

In **[SOP Arena](https://sharedcode.github.io/sop-arena/)**, every control maps directly to a real distributed systems concept:

| Simulation Control | Distributed Systems Concept | SOP Technical Mechanism |
| :--- | :--- | :--- |
| **Add Worker** | Swarm Compute | Dynamic queue rebalancing across peer worker nodes without central master bottlenecks. |
| **Remove Worker** | Graceful Degradation | Active tasks drained and re-assigned to healthy nodes with zero dropped writes. |
| **Kill Node / Storage Fault** | Fault Tolerance | **Reed-Solomon Erasure Coding** reconstructs missing B-Tree blocks in-memory from parity chunks. |
| **Transaction Storm** | Concurrency & Isolation | **Optimistic Concurrency Control (OCC)** serializes conflicting writes in microseconds. |
| **Increase Workload (100k TPS)** | Scalability | B-Tree node segments partition write load across sector-aligned storage handles. |
| **Automatic Self-Healing** | Resilient Coordination | Heartbeat lease detection triggers automated task redistribution in `<15ms`. |

---

## 💻 For Developers

### 1. In-Memory Quickstart (Zero Dependencies)

```bash
# Clone the repository and run the quickstart
git clone https://github.com/sharedcode/sop.git
cd sop
go run ./examples/quickstart
```

```go
package main

import (
	"fmt"
	"github.com/sharedcode/sop/inmemory"
)

func main() {
	// Create an in-memory B-Tree with unique integer keys and string values
	tree := inmemory.NewBtree[int, string](true)

	// Add records
	tree.Add(101, "Build #101: tests passed")
	tree.Add(102, "Build #102: deployed to staging")

	// Ordered range scan (keys 100 to 105)
	for k, v := range tree.Range(100, 105) {
		fmt.Printf("Key %d -> %s\n", k, v)
	}
}
```

### 2. AI Agent Memory & Swarm Task Hand-off

Run the new dedicated Agent Memory demo:

```bash
go run ./examples/agent_memory
```

This demo demonstrates an AI worker creating a context checkpoint, crashing mid-step, rolling back cleanly, and having a healthy peer worker resume the task in `<15ms`.

---

## ⚡ Performance Benchmarks

Below are authentic benchmark results from the repository benchmark harness (`tools/benchmark`) running on a standard workstation:

### Tuning `SlotLength` (Items per B-Tree Node)

#### 10,000 Items Benchmark
| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 107,652 | 136,754 | 40,964 |
| **2,000 (Balanced)** | **132,901** | **142,907** | **50,093** |
| 3,000 | 135,066 | 137,035 | 49,754 |
| 4,000 | 123,190 | 122,228 | 48,094 |

#### 100,000 Items Benchmark
| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 121,139 | 145,195 | 48,346 |
| 2,000 | 132,805 | 136,684 | 51,817 |
| 3,000 | 137,296 | 141,764 | 50,605 |
| **4,000 (Write-Heavy)** | **145,417** | 143,770 | **51,988** |

---

## 👥 For Engineering Leaders & Hiring Managers

For technical leaders, CTOs, and hiring managers, this repository serves as a working demonstration of systems engineering across:

- **Storage Engine Design**: Custom B-Tree implementation with sector-aligned direct I/O, node slot tuning, and multi-tier L1/L2 caching.
- **Transactional Systems**: Strict ACID guarantees, Write-Ahead Logging (WAL), Two-Phase Commit (2PC), Snapshot Isolation, and Optimistic Concurrency Control.
- **Fault Tolerance & Reliability**: Reed-Solomon Erasure Coding (N+K striping), active/passive metadata redundancy, and automated partition healing.
- **High Concurrency**: Lock-free data structures, multi-goroutine worker swarms, and SIMD vector dot-product calculation.
- **Polyglot Architecture**: Native Go kernel, Python bindings (`sop4py`), C# bindings (`Sop`), and browser WebAssembly.
- **Production Delivery**: GitHub Actions CI/CD matrix, distroless container builds on GHCR, Codecov integration, and static GitHub Pages deployments.

If you are building distributed systems, cloud infrastructure, or AI data platforms and want to discuss architecture, feel free to connect via [GitHub Discussions](https://github.com/SharedCode/sop/discussions).

---

## 📦 Language Packages & Tooling

| Language | Installation | Description |
| :--- | :--- | :--- |
| **Go** | `go get github.com/sharedcode/sop` | Native high-performance core engine. |
| **Python** | `pip install sop4py` | Python bindings with Data Manager and AI scripts. |
| **C#** | `dotnet add package Sop` | Complete .NET Core integration. |
| **WebAssembly** | `GOOS=js GOARCH=wasm go build` | Browser-sandboxed zero-server execution. |
| **HTTP Data Manager** | `sop-httpserver` | Standalone UI console and AI Copilot interface. |

---

## 📚 Technical Reference Guides

- **[What is SOP, in Plain Words](docs/WHAT_IS_SOP.md)**: High-level conceptual overview.
- **[Architecture Whitepaper](docs/SOP_ARCHITECTURE_WHITEPAPER.md)**: Deep dive into B-Tree layout and transactions.
- **[Platform Tools & Relational Intelligence](docs/SOP_PLATFORM_TOOLS.md)**: Data Manager, CEL expressions, and AI Copilot.
- **[AI Copilot & Agent Architecture](docs/AI_COPILOT.md)**: Multi-agent memory model and Space partitioning.
- **[Operations & Failover Guide](docs/OPERATIONS.md)**: Erasure coding, recovery, and cluster management.
- **[Scalability & Capacity Math](docs/SCALABILITY.md)**: Architectural scaling model for billions of items.

---

## 🤝 Get Involved

We welcome feedback, issues, and contributions:

1. **Fork & Clone**: `git clone https://github.com/sharedcode/sop.git`
2. **Run Tests**: `go test -v ./...`
3. **Join Discussions**: [GitHub Discussions](https://github.com/SharedCode/sop/discussions)
4. **Submit a PR**: Follow Go formatting standards (`gofmt`) and include test coverage.

---

<p align="center">
  <sub>Licensed under the Apache-2.0 License. Built by <a href="https://github.com/sharedcode">SharedCode</a>.</sub>
</p>
