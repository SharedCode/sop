# SOP: Scalable Objects Persistence

## One engine for data and compute.

**SOP** is an ACID-compliant, embedded B-Tree storage engine and distributed computing platform. It unites **transactional persistence**, **ordered key-value storage**, **high-dimensional vector search**, and **swarm compute coordination** into a single cohesive programming model—eliminating the multi-component infrastructure tax of standalone database daemons, message queues, and external distributed lock managers.

[![Discussions](https://img.shields.io/github/discussions/SharedCode/sop)](https://github.com/SharedCode/sop/discussions)
[![CI](https://github.com/SharedCode/sop/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/SharedCode/sop/actions/workflows/go.yml)
[![Release](https://img.shields.io/github/v/release/SharedCode/sop)](https://github.com/SharedCode/sop/releases)
[![codecov](https://codecov.io/gh/SharedCode/sop/branch/master/graph/badge.svg)](https://app.codecov.io/github/SharedCode/sop)
[![Go Reference](https://pkg.go.dev/badge/github.com/sharedcode/sop.svg)](https://pkg.go.dev/github.com/sharedcode/sop)
[![Go version](https://img.shields.io/github/go-mod/go-version/SharedCode/sop)](go.mod)
[![License](https://img.shields.io/github/license/SharedCode/sop)](LICENSE)
[![Live Demos](https://img.shields.io/badge/Live_Demos-GitHub_Pages-10B981?logo=github)](https://sharedcode.github.io/sop-arena/)

---

## ⚡ Experience SOP (Live Demos)

Explore SOP live directly in your browser with **zero installation** and **zero server dependencies**:

| Experience | Description | Live Interactive Link |
| :--- | :--- | :--- |
| 🚀 **SOP Technical Demo** | **Client-Side Zero-Server Go/WASM Engine**<br>Execute live ACID transactions, SIMD vector cosine similarity searches, and latency benchmarks running 100% inside your browser's V8 WebAssembly sandbox with **0 HTTP network calls**. | [**Launch Technical Demo →**](https://sharedcode.github.io/sop/) |
| 🎮 **SOP Arena** | **The Distributed Systems Survival Game**<br>Command a live digital infrastructure. Scale worker swarms, inject storage hardware faults, trigger transaction storms, and watch SOP automatically redistribute tasks and rebuild parity in real-time. | [**Play SOP Arena →**](https://sharedcode.github.io/sop-arena/) |

<p align="center">
  <img src="docs/assets/sop-demo.gif" alt="SOP Interactive Demos and Distributed Systems Architecture Showcase" width="800" />
</p>

---

## 💡 The Thesis

Modern applications increasingly require the tight coupling of **persistent state** with **distributed computation**. 

Today, engineering teams assemble this by stitching together 4 to 6 discrete infrastructure services—databases, message brokers, caching tiers, distributed lock managers, and retry sidecars. 

**SOP explores a unified programming model where data, transactions, coordination, and computation operate within one cohesive engine.** The goal is to reduce architectural complexity, eliminate custom glue code, and achieve sub-millisecond execution while preserving strict serializability and fault-tolerant behavior.

---

## ⚠️ The Problem: The Multi-Component Tax

Most modern distributed architectures look like this:

```
THE FRAGMENTED MULTI-COMPONENT STACK (Without SOP):

[ Application ]
       │
       ├──► (TCP Hop 1: 5-15ms)  ──► Redis (Distributed Locks & Leases)
       ├──► (TCP Hop 2: 5-15ms)  ──► RabbitMQ / Kafka (Task Queue)
       ├──► (TCP Hop 3: 10-30ms) ──► PostgreSQL / Cassandra (Persistent Storage)
       └──► (Failover Glue)      ──► ZooKeeper / Custom Retry & Outbox Daemons

⚠️ 6+ infrastructure boundaries | 15–50ms latency tax | High split-brain failure risk | 60% engineering time on glue code
```

When a worker crashes between releasing a lock in Redis and committing to PostgreSQL, state enters an unrecoverable split-brain condition. Engineering teams spend the majority of their time building and maintaining outbox listeners, lock renewers, and compensating transactions.

### The SOP Unified Approach

```
THE UNIFIED DATA & COMPUTE PLATFORM (With SOP):

[ Application ]
       │
       └──► (Embedded Call / Zero Network Overhead: < 0.3ms)
            ┌─────────────────────────────────────────────────────────────┐
            │                         SOP ENGINE                          │
            │  • Persistent B-Tree Storage (Sector-aligned Direct I/O)    │
            │  • Strict Serializable ACID Transactions (WAL + 2PC)       │
            │  • Swarm Compute & Autonomous Task Redistribution           │
            │  • High-Dimensional Vector Similarity Indexing (SIMD)       │
            │  • Reed-Solomon Erasure Coding & Partition Resilience       │
            └─────────────────────────────────────────────────────────────┘

✓ 1 Single Engine | Sub-millisecond execution | 100% ACID consistency | Automatic failover & self-healing
```

---

## 🎮 See It. Break It. Understand It.

In **[SOP Arena](https://sharedcode.github.io/sop-arena/)**, every gameplay mechanic directly models a core distributed systems capability:

| Demo Action | Distributed Systems Concept | SOP Technical Mechanism |
| :--- | :--- | :--- |
| **Add Worker** | Swarm Compute | Dynamic queue rebalancing across peer worker nodes without central master bottlenecks. |
| **Remove Worker** | Graceful Degradation | Active tasks drained and re-assigned to healthy nodes with zero dropped writes. |
| **Kill Node / Storage Fault** | Fault Tolerance | **Reed-Solomon Erasure Coding** reconstructs missing B-Tree blocks in-memory from parity chunks. |
| **Transaction Storm** | Concurrency & Isolation | **Optimistic Concurrency Control (OCC)** serializes conflicting writes in microseconds. |
| **Increase Workload (100k TPS)** | Scalability | B-Tree node segments partition write load across sector-aligned storage handles. |
| **Automatic Self-Healing** | Resilient Coordination | Heartbeat lease detection triggers automated task redistribution in `<15ms`. |

---

## 📈 Why SOP Could Matter (Market & Architectural Opportunity)

SOP is designed to explore high-impact architectures where co-locating data, compute, and transactions provides distinct structural advantages:

### 1. 🤖 Persistent State for Distributed AI Workloads *(Flagship Opportunity)*
Multi-agent LLM systems require fast memory checkpointing, vector similarity search, and task distribution. Traditional architectures require an orchestration framework + Redis for state + Pinecone for vectors + Postgres for logs + RabbitMQ for queues. If an agent crashes mid-thought, memory context is fragmented.  
*SOP enables AI agents to commit prompt context, embedding vectors, and execution state in a single atomic transaction, redistributing reasoning tasks seamlessly upon node failure.*

### 2. 🕹️ Real-Time Systems & Multiplayer State
Game servers and spatial applications struggle to persist player state, inventories, and matchmaking events at 60Hz without database bottlenecks.  
*SOP embeds directly inside the server process, providing sub-millisecond ordered B-Tree storage and strict serializability.*

### 3. 🏦 Sub-Millisecond Financial & Escrow Ledgers
Financial systems require absolute transactional invariants (e.g. net delta across accounts == $0.00).  
*SOP executes multi-account transactions locally using Write-Ahead Logging (WAL) and Snapshot Isolation, verifying invariants before commit with zero network overhead.*

### 4. 🌐 Decentralized Edge & IoT Swarms
Edge devices in manufacturing, autonomous vehicles, or telecommunications operate with intermittent connectivity and cannot rely on central cloud databases.  
*SOP compiles to standalone binaries and WebAssembly, enabling edge devices to store, query, and transact locally with full ACID compliance, synchronizing with peer swarms when online.*

### 5. ⚡ Serverless Microservices Without Database Bottlenecks
Serverless functions (AWS Lambda, Cloudflare Workers) frequently exhaust database connection pools.  
*SOP embeds within the worker runtime with local NVMe/object-storage persistence, eliminating connection pool limits entirely.*

---

## 🛠️ What This Project Demonstrates (Engineering Depth)

For engineering leaders, hiring managers, and technical evaluators, this repository represents senior/staff-level systems engineering across:

- **Storage Engine Architecture**: Custom B-Tree implementation with configurable node slotting (`SlotLength`), sector-aligned direct I/O, node-segmentation, and multi-tier L1/L2 caching.
- **Transactional Systems**: Strict ACID guarantees, Write-Ahead Logging (WAL), Two-Phase Commit (2PC), Snapshot Isolation, and Optimistic Concurrency Control (OCC).
- **Fault Tolerance & Reliability**: Reed-Solomon Erasure Coding (N+K data/parity striping), active/passive metadata redundancy, and automated partition healing.
- **Concurrency & Concurrency Control**: Lock-free structures, multi-goroutine worker pools, microsecond latency profiling, and SIMD vector dot-product calculation.
- **Polyglot & WebAssembly Systems**: Native Go kernel, Python (`sop4py`), C# bindings, and browser-sandboxed WebAssembly compilation via `syscall/js`.
- **DevOps & Production Engineering**: GitHub Actions CI/CD matrix, automated GitHub Pages deployments, GHCR container delivery, Codecov integration, and comprehensive stress benchmarks.

---

## 🏛️ Technical Architecture Overview

```
                            ┌─────────────────────────────────────────┐
                            │            Application Layer            │
                            │       (Go / Python / C# / WASM)         │
                            └────────────────────┬────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                             SOP ENGINE                                              │
│                                                                                                     │
│   ┌───────────────────────────┐   ┌───────────────────────────┐   ┌─────────────────────────────┐   │
│   │     B-Tree Data Store     │   │     ACID Transaction      │   │     Distributed Swarm       │   │
│   │  • Ordered Key Navigation │   │  • Write-Ahead Log (WAL)  │   │  • Heartbeat Leases         │   │
│   │  • Range Queries & Scans  │   │  • Snapshot Isolation     │   │  • Task Redistribution      │   │
│   │  • Variable Slot Lengths  │   │  • 2-Phase Commit (2PC)   │   │  • Zero-Master Coordination │   │
│   └─────────────┬─────────────┘   └─────────────┬─────────────┘   └──────────────┬──────────────┘   │
│                 │                               │                                │                  │
│                 └───────────────────────┬───────┴────────────────────────────────┘                  │
│                                         ▼                                                           │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │                        Storage Abstraction & Fault Tolerance Tier                           │   │
│   │  • Reed-Solomon Erasure Coding (Data + Parity Sharding)                                     │   │
│   │  • In-Memory Fast Path / Local NVMe / Cloud Object Store (S3, Cassandra, Redis Adapters)    │   │
│   │  • Partitioned High-Dimensional Vector Embedding Index                                      │   │
│   └─────────────────────────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Quickstart

### 1. In-Memory B-Tree (Zero Configuration)
The fastest way to see SOP in action locally:

```bash
# Clone and run the quickstart example
git clone https://github.com/sharedcode/sop.git
cd sop
go run ./examples/quickstart
```

### 2. Language Packages

Install SOP directly into your application ecosystem:

| Language | Installation | Description |
| :--- | :--- | :--- |
| **Go** | `go get github.com/sharedcode/sop` | Native high-performance core engine. |
| **Python** | `pip install sop4py` | Full Python bindings with Data Manager & AI Scripts. |
| **C#** | `dotnet add package Sop` | Complete .NET Core integration. |
| **WebAssembly** | `GOOS=js GOARCH=wasm go build` | In-browser zero-server execution. |

### 3. Launching the SOP Data Manager

```bash
# Included with language packages or downloadable from GitHub Releases
sop-httpserver
```

---

## ⚡ Performance & Optimization Guide

SOP is designed for high throughput and low latency. Below are baseline benchmark results running on a standard workstation using the built-in benchmark tool (`tools/benchmark`):

### Tuning `SlotLength` (B-Tree Node Density)

The `SlotLength` parameter controls the number of items stored in each B-Tree node:

#### 10,000 Items Benchmark
| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 107,652 | 136,754 | 40,964 |
| **2,000 (Recommended)** | **132,901** | **142,907** | **50,093** |
| 3,000 | 135,066 | 137,035 | 49,754 |
| 4,000 | 123,190 | 122,228 | 48,094 |

#### 100,000 Items Benchmark
| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 121,139 | 145,195 | 48,346 |
| 2,000 | 132,805 | 136,684 | 51,817 |
| **4,000 (Write-Heavy)** | **145,417** | **143,770** | **51,988** |
| 5,000 | 137,054 | 144,565 | 50,309 |

---

## 📚 Technical Reference Guides

- **[What is SOP, in Plain Words](docs/WHAT_IS_SOP.md)** — High-level conceptual guide.
- **[Architecture Whitepaper](docs/SOP_ARCHITECTURE_WHITEPAPER.md)** — Deep dive into B-Tree layout and transactions.
- **[Platform Tools & Relational Intelligence](docs/SOP_PLATFORM_TOOLS.md)** — Data Manager, CEL expressions, and AI Copilot.
- **[AI Copilot & Agent Architecture](docs/AI_COPILOT.md)** — Multi-agent memory model and Space partitioning.
- **[Operations & Failover Guide](docs/OPERATIONS.md)** — Erasure coding, recovery, and cluster management.
- **[Scalability & Capacity Math](docs/SCALABILITY.md)** — Architectural scaling model for billions of items.

---

## 🤝 Get Involved & Contributing

We welcome contributions from distributed systems engineers, storage architects, and developers:

1. **Fork & Clone** the repository: `git clone https://github.com/sharedcode/sop.git`
2. **Run Tests**: `go test -v ./...`
3. **Explore Issues**: Check out [GitHub Issues](https://github.com/sharedcode/sop/issues) and [Discussions](https://github.com/sharedcode/sop/discussions).
4. **Submit a Pull Request**: Follow standard Go formatting (`gofmt`) and include test coverage.

---

## 👥 For Engineering Leaders & Investors

If you are interested in engineers who design and build across **distributed systems, embedded storage kernels, cloud infrastructure, and AI data systems**, SOP represents the caliber of deep systems engineering and product intuition being developed here.

- 💬 **Discussions & Feedback:** [Join GitHub Discussions](https://github.com/SharedCode/sop/discussions)
- 🌐 **Project Home:** [sharedcode.github.io/sop](https://sharedcode.github.io/sop/)
- 🎮 **Interactive Arena:** [sharedcode.github.io/sop-arena](https://sharedcode.github.io/sop-arena/)

---

<p align="center">
  <sub>Licensed under the Apache-2.0 License. Built with passion by <a href="https://github.com/sharedcode">SharedCode</a>.</sub>
</p>
