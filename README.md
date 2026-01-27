# Scalable Objects Persistence (SOP) Data & Compute Platform

[![Discussions](https://img.shields.io/github/discussions/SharedCode/sop)](https://github.com/SharedCode/sop/discussions) [![CI](https://github.com/SharedCode/sop/actions/workflows/go.yml/badge.svg?branch=master)](https://github.com/SharedCode/sop/actions/workflows/go.yml) [![codecov](https://codecov.io/gh/SharedCode/sop/branch/master/graph/badge.svg)](https://app.codecov.io/github/SharedCode/sop) [![Go Reference](https://pkg.go.dev/badge/github.com/sharedcode/sop.svg)](https://pkg.go.dev/github.com/sharedcode/sop) [![Go Report Card](https://goreportcard.com/badge/github.com/sharedcode/sop)](https://goreportcard.com/report/github.com/sharedcode/sop)

**SOP** is a comprehensive **Data & Compute Platform** designed for the modern enterprise and high-performance applications.

At its core, SOP is not just a traditional code library; it is a **Distributed Computing Framework** that powers **Swarm Computing**—the efficient, coordinated management of data and compute across clusters and embedded systems. SOP enables applications to effortlessly scale from a single device to a massive, distributed "swarm" of intelligent nodes.

### The SOP Ecosystem

Packed inside the library is everything you need to build next-generation distributed systems:
*   **Swarm Computing Engine**: A framework for distributed coordination, allowing applications to act as coherent parts of a greater whole.
*   **Polyglot Storage Engine**: An ACID-compliant B-Tree storage system with Caching and Erasure Coding, optimized for performance. [Read the Architecture Whitepaper](SOP_ARCHITECTURE_WHITEPAPER.md).
*   **AI Scripting & Computing Engine**: A versatile runtime that allows for creating intelligent, self-correcting workflows.

## Installation & Distribution

SOP is designed to be accessible regardless of your preferred technology stack. There are two primary ways to get the **SOP Platform Suite**, which includes both the **SOP Code Library** and the **Platform Tools** (Data Manager, Script VDE, AI Copilot).

### Option 1: Language Packages
Best for developers who want to integrate SOP directly into their application code. Installing the package for your language automatically includes the Data Manager (`sop-httpserver`) and CLI tools.

| Language | Installation | Description |
| :--- | :--- | :--- |
| **Python** | `pip install sop4py` | Full Python bindings with Data Manager & AI Scripts. |
| **Go** | `go get github.com/sharedcode/sop` | The core native library for maximum performance. |
| **C#** | `dotnet add package Sop` |  Complete .NET Core integration. |
| **Java** | *Maven/Gradle* | (Coming Soon) Full JVM support. |
| **Rust** | `cargo add sop` | (Coming Soon) High-performance Rust bindings. |

## AI Copilot Configuration

The **SOP Data Manager** includes an embedded AI Copilot powered by LLMs (like Google Gemini). To use the Copilot features, you must configure your API key.

### Setting the API Key

You can provide the API Key via an environment variable or the configuration file. For a detailed guide on using the conversational interface, case sensitivity rules, and query examples, please refer to the [AI Copilot User Guide](ai/AI_COPILOT_USAGE.md).

**Option 1: Environment Variable (Recommended)**
Set the `SOP_LLM_API_KEY` variable in your shell or `.env` file before launching the application.

```bash
export SOP_LLM_API_KEY="your-api-key-here"
```

**Option 2: Config File**
Add the `llm_api_key` field to your `config.json` file.

```json
{
  "llm_api_key": "your-api-key-here",
  "port": 8080
}
```

*Note: The Copilot requires an active internet connection to communicate with the LLM provider.*


### Option 2: Standalone Binary (GitHub Releases)
Best for infrastructure administrators, DevOps, or "Data-First" users who want to set up the management console immediately. You can download the `sop-httpserver` executable directly from our **[GitHub Releases Page](https://github.com/SharedCode/sop/releases)**.

*   **Universal Manager**: The standalone binary acts as a central console to manage databases created by *any* language binding (Python, C#, Go, etc.).
*   **Zero-Dependency**: No language runtimes (Python/DotNet) required to run the tool itself.
*   **Complete Bundle**: The release archive includes not just the server binary, but also the **native binaries** for language bindings and essential documentation (READMEs), making it a self-contained "SOP Starter Kit".

---

## Development Workflows: Code-First vs. Data-First

The SOP Platform empowers you to choose the starting point that fits your style. Both approaches are **equally powerful** and allow you to manage your databases using the SOP library. It is simply a matter of preference where you begin.

### 1. Code-First Approach
*Start in your IDE.*

In this workflow, you use the **SOP Code Library** to define your data structures and logic programmatically.
1.  **Define & Run**: You write code to define B-Tree stores, transaction logic, and data types (Go, Python, C#, etc.). When your app runs, SOP creates the necessary structures on disk.
2.  **Manage Later**: You can then launch the **Data Manager** to inspect, query, and visualize the data your application has created. This is perfect for developers who prefer to stay in code and treat the database as an embedded component.

### 2. Data-First Approach
*Start in the Visual Tool.*

In this workflow, you start with the **Data Manager** to define your schema and data beforehand.
1.  **Visual Design**: Use the **Data Manager's Visual Tools** to create Databases, define Stores, and optionally populate initial datasets or manage content.
2.  **Consume in Code**: Your application code simply "opens" these pre-existing stores. This allows you to define the schema and indices (data/schema first) before a single line of business logic is written.

### The Complementary Cycle
These workflows are not mutually exclusive—they represent a full lifecycle.
*   **Start in App -> Continue in Manager**: Build your app, then use the Data Manager to inspect production data, debug issues, or analyze performance.
*   **Start in Manager -> Consume in App**: Prototype a schema or populate a test dataset visually, then hand it off to developers to build the application logic.

SOP ensures seamless interoperability regardless of where you begin.

## SOP Data Manager & AI Suite

SOP allows you to interact with your data using the **SOP Data Manager**—a web-based console that features a powerful **SQL-like** query engine and an **AI Copilot**.

### Data Manager Capabilities
*   **Visual Management**: Inspect B-Trees, manage Stores (Key-Value, Vector, Model), and explore the System DB.
*   **Environment Manager**: Switch between environments (Dev, QA, Prod) instantly. Configurations (including the list of databases and connection details) are stored in portable **JSON files**.
*   **Shared Intelligence**: Manage permissions and connections to share databases across the network, allowing different teams to collaborate on the same "System Knowledge" base.

### Designing for AI: Relations
SOP is not just a storage engine; it is designed to be the "Long Term Memory" for AI Agents.
There are two ways to model relationships so that AI Agents can intuitively navigate your data:

1.  **Direct Relations (Metadata)**: Use this for standard One-to-Many relationships (e.g., `Order.user_id` -> `User.id`). By registering this relationship in the `StoreOptions`, the AI understands the detailed schema and can perform high-performance Joins automatically.
    *   **Pro Tip**: This enables **Bi-Directional Querying** (Parent $\leftrightarrow$ Child) without the need for redundant "Link Stores" or double-indexing. Query "User's Orders" or "Order's User" with equal efficiency.
2.  **Link Store Pattern (Advanced)**: Use this for Many-to-Many relationships or complex graph traversals. Create a dedicated `Link Store` (e.g., `User_Orders`) to map IDs effectively without modifying the base tables.

This structure allows AI Agents to navigate data using simple "Chain of Thought" reasoning steps (e.g., "First find the User ID, then look up their Orders") rather than struggling to generate complex SQL Joins. The SOP Data Manager provides first-class support for visualizing and debugging these relationships.
*   **SQL Capabilities**: Perform familiar SQL operations directly on your NoSQL B-Trees:
    *   **SELECT / SCAN**: Filter data using rich criteria (`$gt`, `$regex`, `$in`).
    *   **JOIN**: Perform high-performance connections between stores (e.g., `Join 'Users' and 'Orders'`).
    *   **CRUD**: Insert, Update, and Delete records via a query interface.

### Storage Distribution & Redundancy
SOP separates the storage responsibilities into two distinct layers, each tunable via the configuration:

1.  **Registry / System Data (Stores Folders)**:
    *   **Configuration**: `StoresFolders` (List of paths).
    *   **Mechanism**: Active/Passive Redundancy.
    *   **Behavior**: You typically provide 2 paths (on different drives). SOP writes to the Active drive. If it fails, the system automatically fails over to the Passive drive to ensure the Registry and System DB remain accessible.

2.  **User Data Files (Erasure Coding)**:
    *   **Configuration**: `ErasureConfigs` (Map of Keys to EC settings).
    *   **Mechanism**: Sharding & Parity (Striping).
    *   **Behavior**: Large data files (User B-Trees, Blobs) are split into chunks (Shards) and distributed across multiple drives. This provides both **Parallel I/O (High IOPS)** and **Fault Tolerance** (e.g., surviving a drive failure via parity reconstruction).
    *   **Key-Based Routing**: You can assign specific stores to specific storage tiers (e.g., "fast-ssd-pool" vs "archive-hdd-pool") using the configuration keys.

### The System Database (SystemDB)
All SOP environments come with a built-in **SystemDB**. Far from just a log repository, this is the "brain" of the platform that stores:
*   **Scripts**: Your automation workflows and compiled functions.
*   **LLM Knowledge**: Standard B-Tree stores containing domain knowledge. Modeled like a "Model Store" (Category + Name keys), we use deterministic lookups to avoid the high false-positive rates found in Vector similarity search.
*   **(Future) RBAC**: Role-Based Access Control configurations for multi-user security.

### AI Copilot & Scripts
The Data Manager includes an integrated AI Copilot that supports **Natural Language Programming**.
*   **Natural Language Queries**: Ask "Show me all active users" or "Join users with their last order", and the system acts on it.
*   **Script Drafting**:
    *   **Draft**: Type `/create my_script` to start a draft.
    *   **Build**: Add steps naturally with `/step` or automatically from your last command.
    *   **Save**: Commit your workflow with `/save`.
*   **Execution**:
    *   **Run**: Execute logic with `/run my_script`.
    *   **Parameters**: Pass dynamic arguments like `/run user_audit id=123`.
*   **Hybrid Engine**:
    *   **Tool Steps**: Preserved as raw code (e.g., `Scan`, `Join`) for **zero-latency** execution.
    *   **Natural Language Steps**: Invokes the LLM only when reasoning is required (e.g., "Analyze sentiment").
*   **Bare-Metal Performance**: Scripts are compiled into efficient engine instructions (Go code), avoiding the overhead of runtime parsing for repeated tasks.

To launch the Data Manager:
```bash
# Data Manager is included in your language binding installation
sop-httpserver
```

## Articles & Deep Dives
*   **[Beyond RAG: The Self-Correcting Enterprise AI](SELF_CORRECTING_AI_ARTICLE.md)** - How SOP turns user corrections into permanent system intelligence.
    *   **New**: Describes "Relational Intelligence" — how the AI reads the graph structure of your stores to perform perfect joins without hallucination.
*   **[SOP vs Modern Database Architecture](SOP_MODERN_DB_ARTICLE.md)** - A deep dive into B-Tree cursors, Zero-Copy Streaming, and Native Joins.
*   **[Programming with SOP](PROGRAMMING_WITH_SOP_ARTICLE.md)** - A conceptual guide to building applications on the SOP platform.
*   **[Scalability & Limits](SCALABILITY.md)** - Understanding the theoretical and practical limits of the system.
*   **[Swarm Computing](SWARM_COMPUTING.md)** - Learn how SOP enables distributed, coordinated computing without a central brain.

## 🚀 Getting Started

**[Download & Installation Guide](GETTING_STARTED.md)**: The fastest way to get up and running with SOP.

## ⚡ Performance

SOP is designed for high throughput and low latency. Below are benchmark results running on a **2015 MacBook Pro** (Dual-Core Intel Core i5, 8GB RAM) using the built-in benchmark tool (`tools/benchmark`).

### Optimization Guide: Tuning `SlotLength`

The `SlotLength` parameter controls the number of items stored in each B-Tree node. Tuning this value can significantly impact performance depending on your dataset size and workload.

**Configuration Used:**
- `IsValueDataInNodeSegment: true` (Values stored directly in leaf nodes)
- `CacheType: sop.InMemory` (Persisted to disk with ACID support)

#### 10,000 Items Benchmark
For smaller datasets, a `SlotLength` of **2,000** offers the best balance.

| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 107,652 | 136,754 | 40,964 |
| **2,000** | **132,901** | **142,907** | **50,093** |
| 3,000 | 135,066 | 137,035 | 49,754 |
| 4,000 | 123,190 | 122,228 | 48,094 |
| 5,000 | 132,670 | 126,432 | 47,150 |

#### 100,000 Items Benchmark
For larger datasets, increasing `SlotLength` to **4,000** yields higher throughput.

| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 121,139 | 145,195 | 48,346 |
| 2,000 | 132,805 | 136,684 | 51,817 |
| 3,000 | 137,296 | 141,764 | 50,605 |
| **4,000** | **145,417** | 143,770 | **51,988** |
| 5,000 | 137,054 | 144,565 | 50,309 |

*Recommendation: Start with a `SlotLength` of 2,000 for general use, and increase to 4,000+ for write-heavy workloads with large datasets.*

> **Pro Tip for Massive Scale:**
> For datasets reaching into the **hundreds of billions or trillions** of records, you can increase `SlotLength` up to **10,000**. This maximizes node density, allowing a single B-Tree to manage petabytes of data with minimal metadata overhead. See [Scalability & Limits](SCALABILITY.md) for the math.

### Why this matters
These benchmarks are running with **Full ACID Transaction** protection. Unlike simple Key-Value stores that optimize purely for random writes (often sacrificing order or safety), SOP provides a robust foundation for complex data access:
- **Sorted Data**: Native support for `ORDER BY ASC/DESC`.
- **Fast Search**: Efficient range scans and lookups.
- **SQL-Ready**: The ordered structure supports efficient Merge Joins and complex query patterns out of the box.
- **Linear Scalability**: SOP is built on **SWARM** (SOP exclusive) transaction technology, allowing it to scale linearly and horizontally across the network as nodes and hardware are added.

### Competitive Landscape

How does SOP compare to other storage engines in the Go ecosystem?

| Database | Type | Typical Batch Write (Ops/Sec) | Key Differences |
| :--- | :--- | :--- | :--- |
| **SOP** | B-Tree | **~145,000** | **ACID, Ordered, SWARM Scalability** |
| **BadgerDB** | LSM Tree | ~150,000 - 300,000 | Faster writes (LSM), but requires compaction and lacks native B-Tree ordering features. |
| **SQLite** | B-Tree (C) | ~50,000 - 100,000 | Slower due to SQL parsing overhead. |
| **BoltDB** | B-Tree | ~10,000 - 50,000 | Slower random writes due to copy-on-write page structure. |
| **Go `map`** | Hash Map | ~10,000,000+ | In-memory only. No persistence, no ACID, no ordering. |

> **Note on SOP's Unique Value Proposition:**
> While raw speed is comparable to top-tier engines, SOP distinguishes itself by combining features that usually don't exist together:
> *   **Full ACID Transactions**: Guarantees safety where others might trade it for speed.
> *   **SWARM Technology**: Unlike monolithic engines, SOP scales linearly across the network.
> *   **SQL-Ready Structure**: Data is stored ordered, enabling `ORDER BY`, range scans, and efficient joins without extra indexing overhead.

## Interoperability & Data Management

SOP is designed as a **Universal Data Platform**. Whether you are writing in Go, Python, Java, C#, or Rust, your data should be accessible, manageable, and interoperable.

### 1. The Universal Approach (JSONDB)
This is the standard approach used by all language bindings (Python, Java, C#, Rust) and is also available to Go developers via the `jsondb` package.
*   **Mechanism**: Data is serialized as JSON.
*   **Benefit**: **Native Data Manager Support**. Since the format is standardized, the SOP Data Manager can automatically read, write, query, and visualize your B-Trees without any extra configuration.
*   **Interoperability**: A B-Tree created in Python can be read by a Java app or managed by the Go-based Data Manager.

### 2. The Native Go Approach (Structs & Comparers)
Go developers often prefer storing native structs for maximum performance and type safety. SOP supports this fully but requires a bridge to be manageable by the generic Data Manager.

*   **SOP as a Library (Code First)**:
    *   **Usage**: You define custom structs and a custom comparer in Go code.
    *   **Pros**: Maximum flexibility and performance.
    *   **Cons**: **Hidden Logic**. The Data Manager cannot inherently understand your compiled sorting logic.

*   **SOP as a Managed Platform (Data First)**:
    *   **Usage**: You provide an **`IndexSpecification`** that describes your key fields and sorting order.
    *   **Pros**: **Full Data Manager Support**. The `IndexSpecification` acts as the contract, allowing the UI and AI Agents to manage your data.
    *   **Workflow**: You can use the **Data Manager** to create the B-Tree and define the `IndexSpecification`. Then, use the built-in **Code Generator** to generate the Go structs.

### Bridging the Gap: From Code-First to Managed (Safe & Zero-Downtime)

SOP supports a powerful hybrid workflow. You can start with a **Code-First** approach (using custom Go structs and comparers) and later "upgrade" your store to be fully manageable by the Data Manager—**without migration or downtime**.

*   **The Feature**: Use the Data Manager's **Edit Store** functionality to attach an `IndexSpecification` or `CEL expression` to an existing "Code-First" store.
*   **The Safety Mechanism**: This operation is **100% safe** but protected.
    1.  **Admin Unlock**: For non-empty stores, these fields are locked by default. You must provide an **Admin Token** to unlock them, ensuring only authorized personnel can modify the schema of a live database.
    2.  **Metadata Only**: The change only updates the **Store Registry** (metadata). It *never* touches the actual B-Tree data nodes or re-writes the data file.
    3.  **Non-Invasive**: The underlying B-Tree structure remains identical. The Data Manager simply uses the new specification to interpret and sort the keys dynamically, matching the logic of your compiled code.
    4.  **Reversibility**: Since the data is untouched, you can refine or remove the specification at any time.

This allows DBAs to take a "black box" store generated by application code and turn it into a transparent, queryable dataset for reporting, debugging, and AI analysis.

### General Purpose & Strong Typing

SOP B-Trees are **general-purpose** storage engines. Similar to a Model Store, they can store "any" value type (e.g., `interface{}` in Go, `Object` in Java).

*   **Dynamic Typing**: You can store mixed types in the same B-Tree if your application logic supports it.
*   **Strong Typing via "Seed"**: To enforce strong typing and enable rich Data Manager features, you should **seed** the B-Tree with an initial item.
    *   **Discovery**: When you add a "seed" item (the first record), the Data Manager inspects it to discover the Key and Value types automatically.
    *   **Schema Enforcement**: This effectively "locks in" the schema for the UI, allowing it to generate correct forms and validation rules.
    *   **Swarm Readiness**: As noted below, this seed item also initializes the tree structure, enabling efficient "Swarm" transaction merging immediately.

See the [API Cookbook](COOKBOOK.md#interoperability-note-go-vs-other-languages) for details.

## SOP Data Manager

SOP now includes a powerful **SOP Data Manager** that provides **full CRUD** capabilities for your B-Tree stores. It goes beyond simple viewing, offering a complete GUI for inspecting, searching, and managing your data at scale.

*   **Web UI**: A modern, responsive interface for browsing B-Trees, managing stores, and visualizing data.
*   **AI Copilot**: Integrated directly into the UI, the AI Copilot can help you write queries (including SQL-like Joins), explain data structures, and even generate code snippets.
*   **Natural Language SQL**: Perform complex Selects, Joins, and CRUD operations using plain English.
*   **Streaming Architecture**: Results from Agents and Scripts are streamed in real-time, enabling **Scripts as Views** and efficient handling of large datasets with minimal memory footprint.
*   **SystemDB**: View and manage internal system data, including registry information and transaction logs.
*   **Scripts**: Record and replay complex data operations for testing or automation (now with Parameter support).

To launch the SOP Data Manager:

```bash
# From the root of the repository
go run ./tools/httpserver
```

Or use the pre-built binaries if available. See [tools/httpserver/README.md](tools/httpserver/README.md) for more details.

## SOP AI Kit

The **SOP AI Kit** transforms SOP from a storage engine into a complete AI data platform.

*   **Vector Store**: Native support for storing and searching high-dimensional vectors.
*   **RAG Agents**: Build Retrieval-Augmented Generation applications with ease.
*   **Scripts**: A functional AI runtime for recording and replaying complex workflows.

See [ai/README.md](ai/README.md) for a deep dive into the AI capabilities.

## Table of contents

- [Introduction](#introduction)
- [Polyglot Support](#polyglot-support)
- [Interoperability (Go vs. Bindings)](#interoperability-go-vs-bindings)
- [Scalability & Limits](#scalability--limits)
- [Swarm Computing (Architecture)](SWARM_COMPUTING.md)
- [Workflows & Scenarios](WORKFLOWS.md)
- [Engineering Culture & SDLC](SDLC.md) - **New!** How we use AI & Automation.
- [High-level features and articles](#high-level-featuresusability-articles-about-sop)
- [Architecture Guide](ARCHITECTURE.md)
- [Configuration & Tuning](CONFIGURATION.md)
- [Operational Guide](OPERATIONS.md)
- [API Cookbook](COOKBOOK.md)
- [Examples (Go)](examples/README.md)
- [Quick start](#quick-start)
- [Lifecycle: failures, failover, reinstate, EC auto-repair](README2.md#lifecycle-failures-failover-reinstate-and-ec-auto-repair)
- [Prerequisites](#prerequisites)
- [Running integration tests (Docker)](#running-integration-tests)
 - [Testing (unit, integration, stress)](#testing-unit-integration-stress)
- [Usability](README2.md#usability)
- [SOP API discussions](README2.md#simple-usage)
- [SOP for Python (sop4py)](bindings/python/README.md)
- [SOP for Java (sop4j)](bindings/java/README.md)
- [SOP for C# (sop4cs)](bindings/csharp/README.md)
- [SOP for Rust (sop4rs)](bindings/rust/README.md)
- [SOP AI Kit](ai/README.md) - **New!** Includes Vector Store, RAG Agents, and **Scripts** (Record & Replay).
  - [AI Script Architecture](ai/AI_SCRIPT_ARCHITECTURE.md) - Deep dive into our scalable, functional AI runtime.
  - [Scripting Guide](ai/SCRIPTS.md) - Learn how to record and write scripts.
- [SOP Data Manager](tools/httpserver/README.md) - **Updated!** A powerful SOP HTTP Server & Web UI that scales from a local embedded viewer to a clustered enterprise management console on Kubernetes.
- [Changelog](CHANGELOG.md)
- [Community & support](#community--support)
- [Contributing & license](#contributing--license)
 - [Coordination model (OOA) and safety](#coordination-model-ooa-and-safety)

## Swarm Computing & Concurrent Transactions
SOP supports "Swarm Computing" where multiple distributed processes or threads can concurrently modify the same B-Tree without external locks. The library handles ACID transactions, conflict detection, and merging automatically.

**Important Requirement for First Commit:**
To enable seamless concurrent merging on a newly created B-Tree, you **must pre-seed the B-Tree with at least one item** in a separate, initial transaction.
- **Why?** This establishes the root node and structure, preventing race conditions that can occur when multiple transactions attempt to initialize an empty tree simultaneously.
- **Bonus**: As mentioned above, this seed item also allows the **Data Manager** to auto-discover the Key/Value types, turning your general-purpose B-Tree into a strongly-typed, manageable store.
- **Note:** This requirement is simply to have at least one item in the tree. It can be a real application item or a dummy seed item.
- **Safety:** Your data remains ACID-compliant. This step simply ensures the "first commit" doesn't suffer from a "random drop" race condition where one transaction's initialization overwrites another's.
- **After this single seed item is committed, the B-Tree is fully ready for high-concurrency "swarm" operations.**

## Cluster reboot procedure
When rebooting an entire cluster running applications that use SOP, follow this order to avoid stale locks and ensure clean recovery:

1) Gracefully stop all apps that use SOP across the cluster.
2) Stop the Redis service(s) used by these SOP apps.
3) Reboot hosts if needed (or proceed directly if not).
4) Start the Redis service(s) first and verify they are healthy.
5) Start the apps that use SOP.

## Self-Healing & Reliability

SOP includes a robust background servicer that ensures database integrity even in the face of infrastructure failures like Redis restarts.

### Redis Restart Detection & Lock Resurrection
In **Clustered mode** (using Redis), SOP employs a minimally intrusive "on Redis restart" detector. This mechanism:
- **Detects Redis Restarts**: Automatically identifies when the Redis cache has restarted or lost volatile data.
- **Resurrects Locks**: If a transaction was incomplete during a Redis failure, the system automatically "resurrects" the necessary locks for the transaction's priority logs.
- **Prevents Corruption**: This ensures that the registry sector does not become corrupted due to half-complete transactions.
- **Self-Healing**: The background servicer automatically handles this lifecycle maintenance, keeping the database "rock solid" without manual intervention.

*Note: This feature is specific to Clustered mode. In **Standalone mode**, the application performs a similar cleanup sweep immediately upon startup.*


Notes:
- SOP relies on Redis for coordination (locks, recovery bookkeeping). Bringing Redis up before SOP apps prevents unnecessary failovers or stale-lock handling during app startup.
- If any node was force-killed, SOP’s stale-lock and rollback paths will repair on next write; starting Redis first ensures that path has the needed state.

# Introduction
What is SOP?

Scalable Objects Persistence (SOP) is a **bare metal storage engine** that bakes together a set of storage related features & algorithms in order to provide the most efficient & reliable (ACID attributes of transactions) technique (known) of storage management and rich search. It brings to the application the raw muscle of "raw storage" via direct I/O communications with disk drives, bypassing the overhead of intermediate database layers.

SOP V2 core is written in **Go**, but provides first-class bindings for **Python**, **Java**, **C#**, and **Rust**, making it a truly universal storage solution. It can be used for storage management by applications of many types across different hardware architectures & Operating Systems (OS).

## Polyglot Support

SOP is designed as a **"Write Once, Run Anywhere"** architecture. The core engine is compiled into a shared library (`libsop.so`/`.dylib`/`.dll`) which is then consumed by language-specific bindings. This ensures that all languages benefit from the same high-performance, ACID-compliant core.

For a deep dive into our multi-language architecture, see [Polyglot Support](MULTI_LINGUAL_SUPPORT.md).

### Supported Languages
*   **[Go](https://pkg.go.dev/github.com/sharedcode/sop)**: The native core. Best for high-concurrency backend services.
*   **[Python (sop4py)](bindings/python/README.md)**: Ideal for AI/ML, RAG applications, and data science.
*   **[Java (sop4j)](bindings/java/README.md)**: Perfect for enterprise backends and legacy modernization.
*   **[C# (sop4cs)](bindings/csharp/README.md)**: Native integration for .NET Core and Windows environments.
*   **[Rust (sop4rs)](bindings/rust/README.md)**: For systems programming and high-performance applications.

## Scalability & Limits
SOP is architected to handle **Petabyte-scale** datasets and **Trillions of objects**.
- **Capacity**: Up to 495 Billion items per segment (with 1,000 segments = 495 Trillion items) **per Btree**.
- **Throughput**: Limited only by hardware (Redis Cluster + Storage I/O), not software.
- **Design**: Horizontal scaling via independent storage nodes and sharded registry.

See the full analysis in [Scalability & Limits](SCALABILITY.md).

## Key Use Cases

SOP is designed to be versatile, powering everything from small embedded tools to massive enterprise clusters.

For detailed architectural patterns, deployment lifecycles, and configuration examples, see [Workflows & Scenarios](WORKFLOWS.md).

### 1. Standalone App (Embedded DB)
*   **Scenario**: Desktop apps, CLI tools, or single-node services needing rich indexing.
*   **Why SOP**:
    *   **Bare Metal Performance**: Direct B-Tree indexing on disk with minimal abstraction overhead.
    *   **Speed**: "NoCheck" transaction mode. For build-once-read-many scenarios, skip conflict checks entirely for raw, unbridled read speed.
    *   **Simplicity**: No external database dependencies (just a local file structure).

### 2. Enterprise Cluster App
*   **Scenario**: Distributed systems requiring high availability and ACID guarantees.
*   **Why SOP**:
    *   **ACID Transactions**: Two-Phase Commit (2PC) across distributed nodes.
    *   **Multi-Tenancy**: Native support for multi-tenancy (via Directories or Keyspaces) allows multiple tenants to share the same cluster while maintaining strict data isolation.
    *   **Scalability**: Infinite metadata scaling via Sharded Registry (FileSystem) or Cassandra tables.
    *   **Resilience**: Registry replication (Active/Passive or Quorum) and Erasure Coding for data blobs ensure zero data loss.
    *   **Operational Flexibility**: Choose the backend that fits your ops stack:
        *   **FileSystem (`infs`)**: The most versatile option. Run on **Local Disk** for embedded/dev use, or mount a **Network Drive (NAS/S3)** for infinite cluster scalability. Requires only a shared mount and Redis.
        *   **Cassandra (`incfs`)**: **"Power up"** your existing Cassandra cluster with SOP. Adds full **ACID Transactions**, **B-Tree Indexing** (ordered data, range queries), and efficient large item management to Cassandra's eventual consistency model.

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
    *   **Unified Storage**: Store your training data (Vectors), metadata (Registry), and model artifacts (Blobs/JSON) in one ACID-compliant system.
    *   **Atomic Updates**: Update your model weights and the vector index they correspond to in a single transaction, preventing version mismatch.
    *   **Versioning**: Built-in support for versioning models (e.g., "v1.0", "v1.1") using composite keys.

### 7. Cassandra Power-Up (Layer 2 Database)
*   **Scenario**: Enhancing existing Cassandra clusters with features it natively lacks.
*   **Why SOP**:
    *   **Solves the "Blob Problem"**: Keeps Cassandra lean by storing only metadata (Registry) in tables, while offloading heavy data (B-Tree nodes, values) to the file system or object storage. This prevents compaction issues common with large blobs.
    *   **Rich Indexing**: Adds full B-Tree capabilities (range queries, prefix search, ordering) which are natively missing or limited in Cassandra.
    *   **ACID Transactions**: Provides strict ACID transactions (Two-Phase Commit) on top of Cassandra's eventually consistent architecture.
    *   **Multi-Tenancy**: Native support for Keyspaces allows logical separation of data within the same cluster.

### 8. Embedded Search Engine
*   **Scenario**: Adding "Search this wiki" or "Filter by text" features to an application without managing a separate Elasticsearch cluster.
*   **Why SOP**:
    *   **Transactional Indexing**: Index documents in the same transaction as you save them. No "eventual consistency" lag.
    *   **BM25 Scoring**: Uses industry-standard ranking algorithms for relevance.
    *   **Zero Ops**: It's just a library. No separate process to manage or monitor.

## Core Innovations

SOP breaks the mold of traditional database architectures by combining the speed of embedded systems with the scalability of distributed clusters.

### 1. The Unified Native Core ("Write Once, Run Everywhere")
Unlike many databases that rely on slow TCP/IP protocols for local drivers, SOP runs **inside your application process**.
*   **Architecture**: The core engine is written in Go and compiled into a high-performance **Shared Library** (`.so` / `.dll`).
*   **Language Agnostic**: Whether you use Python, C#, or Java, your application loads this engine directly into memory using FFI.
*   **Zero Latency**: Function calls (e.g., `db.Get()`) are direct memory operations, not network requests. This allows for millions of operations per second on a single node.

### 2. Swarm Computing Architecture
SOP is not just a storage engine; it is a **coordination framework**.
*   **Distributed State**: Nodes in a cluster don't just store data; they share **System Knowledge**.
*   **Virtual Execution**: The "OS" of the swarm allows you to run distributed workflows where logic moves to the data, rather than moving data to the logic.
*   **Linear Scalability**: Add more nodes to increase both storage capacity (Storage Nodes) and processing power (Compute Nodes) linearly.

### 3. Rich Key Structures (Metadata-Carrying Keys)
Standard Key-Value stores treat keys as opaque strings. SOP treats them as **First-Class Objects**.
*   **Complex Keys**: You can use complex structs (e.g., `{Region, Dept, EmployeeID}`) as keys.
*   **"Ride-on" Data**: Critical metadata (like `Version`, `Deleted` flags, or `CentroidID` for vectors) is physically stored in the B-Tree Key node.
    *   **Performance**: Operations like "List all non-deleted items" scan only the lightweight index, avoiding the expensive I/O of fetching the full data payload.
    *   **Consistency**: This metadata serves as the source of truth for ACID transactions.

### 4. Hybrid AI Compute & Scripting (Self-Correcting)
SOP introduces a novel paradigm by embedding a **Scripting Engine directly into the AI Run-Loop**.
*   **Compiled Instructions**: Unlike chat-only bots, SOP allows you to "record" complex AI reasoning into **deterministic scripts**. Once verified, these scripts run as compiled programs, eliminating future hallucinations for that task.
*   **Self-Correction with Memory**: Agents possess controlled **Short-Term (Context)** and **Long-Term (Vector Store)** memory, enabling them to learn from mistakes and refine their own scripts over time ("Self-Correction").
*   **Safety & Adoption**: By freezing successful stochastic reasoning into deterministic code, SOP removes the risk of "hallucinations" in production, enabling **Safe AI Adoption** across regulated industries like Healthcare and Finance.
*   **Exponential Automation**: This unique combination of properties unlocks a **Tenfold Increase in Automatable Tasks**, allowing systems to handle complex, multi-step workflows that were previously considered too risky or complex for AI.

### 5. Granular Durability & RAID
Moving beyond simple replication, SOP brings hardware-level reliability concepts into software.
*   **Erasure Coding**: Split large objects (Blobs) across multiple physical drives or network locations with parity, ensuring data survival even if multiple drives fail.
*   **Store-Level RAID**: You can configure redundancy policies **per-store**. Your "Logs" store can be ephemeral (fast, low safety), while your "Financials" store uses Reed-Solomon Erasure Coding (maximum safety) on the same cluster.

For a deeper dive into the system's design and package structure (including the Public vs. Internal split), please see the [Architecture Guide](ARCHITECTURE.md).

For configuration options and performance tuning, see the [Configuration Guide](CONFIGURATION.md).

For operational best practices (failover, backups), see the [Operational Guide](OPERATIONS.md).

For code examples, check out the [API Cookbook](COOKBOOK.md).

See more details here that describe further, the different qualities & attributes/features of SOP, and why it is a good choice as a storage engine for your applications today: [Summary](README2.md)

Before I go, I would like to say, SOP is a green field, totally new. What is being shipped in V2 is just the start of this new product. We are barely scratching the surface of what can be done that will help storage management at super scale. SOP is a super computing enabler. The way its architecture was laid out, independent features and together, they are meant to give us the best/most efficient performance & IO of a group of computers (cluster), network & their storage, that can possibly give us.

## High level features/usability articles about SOP
- [**New!** AI Copilot: A "Local Expert" for your Data](AI_COPILOT.md) - How we built a secure, RAG-based agent into the SOP Data Manager.
- [SOP: The Modern Database Library](SOP_MODERN_DB_ARTICLE.md) - Why "Database-as-a-Library" is the future of cloud-native storage.

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
  opts := sop.DatabaseOptions{
      Type:          sop.Standalone,
      StoresFolders: []string{"/var/lib/sop"},
  }

  // Start a Transaction
  tx, _ := database.BeginTransaction(ctx, opts, sop.ForWriting)

  // Open a Store (B-Tree, Vector, or Model)
  users, _ := database.NewBtree[string, string](ctx, opts, "users", tx, nil)

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
Each write or read transaction opportunistically invokes an internal onIdle() path at the start (`Begin()`). This lightweight pass performs two independent maintenance tasks:

1. Priority rollback sweeps (Lock Resurrection): Resurrects lost locks for interrupted higher-priority transactions by consulting per‑transaction priority log (.plg) files. This allows transactions to "self-heal" by enabling stale detection and rollback as necessary.
	- Cluster-wide coordination: This task is coordinated across the entire cluster (or all threads in standalone mode). Only one worker "wins" and performs the sweep at any given time, ensuring no redundant processing. This prevents unnecessary "swarm overload" on these onIdle services.
	- Restart fast path: On application start (in embedded mode), SOP triggers a one‑time sweep of all priority logs immediately, ignoring age. This accelerates recovery of any half‑committed writes that were waiting for the periodic window. In **Clustered mode**, a global coordinator ensures that only one process in the entire cluster pings the Redis 'notrestarted' flag and performs the actual lock resurrection service if needed.
	- Periodic path: Absent a restart, one worker periodically processes aged logs. Base interval is 5 minutes. If the previous sweep found work, a shorter 2 minute backoff is used to drain backlog faster. Intervals are governed by two atomically updated globals: lastPriorityOnIdleTime (Unix ms) and priorityLogFound (0/1 flag).
	- **Rule**: Priority logs older than 5 minutes are considered "abandoned" and are rolled back by this servicer.
	- Concurrency: A mutex plus atomic timestamp prevents overlapping sweeps; only one goroutine performs a rollback batch at a time even under high Begin() concurrency.
	- Rationale: Using onIdle piggybacks maintenance on natural transaction flow without a dedicated background goroutine, simplifying embedding into host applications that manage their own scheduling.

2. Expired transaction log cleanup: Removes obsolete commit/rollback artifacts (B-Tree node pages and data value pages).
	- Cluster-wide coordination: Like priority sweeps, this task is coordinated cluster-wide. Only one worker wins the right to perform the cleanup for a given interval (regular or accelerated).
	- **Intervals**:
		- **4 Hours (Default)**: B-Tree nodes and data pages modified in a transaction are temporary until the commit updates the Registry. Since the Registry is the source of truth for ACID transactions, cleaning up these temporary artifacts can be done at a "luxury of time" pace (4 hours) without affecting data integrity.
		- **5 Minutes (Accelerated)**: If recent activity suggests potential pending cleanup (e.g., known rollbacks), the interval accelerates to 5 minutes to reclaim space faster.
	- Timing uses an atomic lastOnIdleRunTime.

Thread safety: Earlier versions used unsynchronized globals; these now use atomic loads/stores (sync/atomic) to eliminate race detector warnings when tests force timer rewinds. Tests that manipulate timing (to speed up sweep scenarios) reset the atomic counters instead of writing plain globals.

Operational impact: You generally do not need to call anything explicitly—just ensure transactions continue to flow. If you embed SOP in a service that may become read‑only idle for long stretches but you still want prompt rollback of higher‑priority interruptions, periodically issue a lightweight read transaction to trigger onIdle.

Testing notes: Unit tests rewind lastPriorityOnIdleTime and priorityLogFound (atomically) to force immediate sweep execution; this pattern is acceptable only in test code. Production code should never reset these values manually.

## Prerequisites
- Go 1.24.3+
- **OS**: macOS, Linux, or Windows.
    - **Architectures**: x64 (AMD64/Intel64) and ARM64 (Apple Silicon/Linux aarch64).
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

## Clustered Mode Compatibility

In **Clustered Mode**, SOP uses Redis to coordinate transactions across multiple nodes. This allows many machines to participate in data management for the same Database/B-Tree files on disk while maintaining ACID guarantees.

**Note**: The database files generated in Standalone and Clustered modes are fully compatible. You can switch between modes as needed but make sure if switching to Standalone mode, that there is only one process that writes to the database files.

## Community & support
- Issues: https://github.com/SharedCode/sop/issues
- Discussions: https://github.com/SharedCode/sop/discussions (design/usage topics)

## Contributing & license
- Contributing guide: see CONTRIBUTING.md
- Code of Conduct: see CODE_OF_CONDUCT.md
- License: MIT, see LICENSE