# Architecture Guide

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

## AI & Cognitive Memory Architecture

SOP introduces a novel architecture for AI Agents, distinguishing itself from standard "RAG" or "Chatbot" implementations by adopting a **Tri-State Cognitive Memory Architecture**. This leverages the B-Tree as the central nervous system, providing session continuity, experiential learning, and Knowledge Base compilation.

### 1. Working Memory (MRU) - Session-Scoped
*   **Mechanism**: A fast, in-memory array (`[]MRUItem`) attached to the `Session` state.
*   **Workflow**: Ensures context is contiguous for the User regardless of which agent is currently active. The Omni Architect orchestrates passing a copied snapshot of the Session MRU into the Avatar as a "whiteboard", returning and appending the result to the global MRU under thread-safe locks.
*   **Context Continuity (Dynamic Semantic Injection)**: Rather than falling back to placeholder text ("dummy" logs) during multi-turn exchanges, the engine dynamically pipes actual semantic RAG chunks (`kb.SearchSemantics`) straight into `MarkMRUCategory()`. If a user's follow-up query is semantically bare, the system seamlessly pulls the `Carried-Over Playbook Context` directly from the prior exchange in the MRU. This reliably eradicates conversational amnesia across the episodic ReAct loop.

### 2. Episodic Memory (STM) - Avatar-Scoped
*   **Mechanism**: A completely physically isolated B-Tree per agent (`stm_<agent_id>`), detaching from global channels.
*   **Workflow**: The Avatar operates a private, localized batch-write loop connected to its own channel. During its internal execution loop, it logs execution snapshots to its channel. A background worker periodically flushes its local channel to the `stm_<agent_id>` tree natively. This guarantees O(1) decommissioning and pure isolation.

### 3. Declarative Long-Term Memory (LTM) & Sleep Cycle
*   **Mechanism**: The Avatar has private Vector DBs (`ltm_<agent_id>`) for its own semantic procedural learnings swept during its private **Sleep Cycle**. By balancing "Minted" UI Knowledge Bases (Declarative) against Auto-Enriched Conversational Memory (Episodic LTM), SOP bridges unstructured interactions into structured vectors through a rigorous summarization, clustering, and fallback-LLM cataloging pipeline.
*   **Direction**: LTM is intended to evolve from a passive vector store into a managed cognitive asset. User interaction should drive what gets captured, Sleep Cycle should refine and re-categorize thoughts, and the resulting LTM should continuously influence future LLM behavior. LTM itself will eventually require MRU/LRU-like lifecycle policies so cold or superseded thoughts can be archived, compacted, or purged while high-value thoughts remain behaviorally active.
*   **Current Default**: The present runtime composition is Avatar + KB + STM + LTM. The KB is still the primary grounding surface, and we need to stabilize exactly how that mounted KB benefits the LLM before making absorption a dominant operating mode.
*   **Knowledge Absorption**: Curated Knowledge Bases should eventually be absorbable into LTM as distilled skills/expertise, giving the AI a durable internal depot of reusable competencies rather than forcing every capability to remain tied to an actively mounted Space.
*   **Long-Term Shift**: This enables a future transition from an "Avatar of a KB" to an Avatar simply carrying STM/LTM plus absorbed expertise, with additional KBs assimilated over time as new competencies.

### 4. Hybrid Scripting Engine (Explicit Execution)
The SOP Scripting Engine (`ai/agent`) follows a unique **"Explicit Execution"** design pattern.
*   **No "Magic" Compilation**: The engine is "dumb and obedient." It does not try to guess user intent or "compile away" interaction steps.
*   **Hybrid Flow**: Scripts naturally mix **Deterministic Commands** (e.g., `scan`, `filter`) with **Probabilistic Reasoning** (e.g., `ask`).
*   **Run-Loop Scripting**: The Agent can pause a deterministic workflow to ask the LLM for guidance ("Analyze these results"), and then resume execution based on the LLM's structured response. This allows for essentially infinite complexity in agentic behaviors without the fragility of pure-LLM loops.

### 5. Architectural Philosophy: Native Semantic Instruction vs Binary Hardcoding
We have strictly banned the practice of hardcoding prompt engineering limits inside the Go application binary (e.g. constant strings in `copilottools.go`). 
*   **Semantic Overrides**: Instead of injecting engine-agnostic DSL parameters (like "NEVER use JSONLogic, use CEL" inside `copilottools.go`), these architectural constraints are codified natively inside the Playbooks/Knowledge Bases (like `SYSTEM_KNOWLEDGE.md` / `sop_base_knowledge.json`).
*   **Dynamic Brain Alignment**: This ensures the "Dynamic Brain" Retrieval-Augmented Generation execution can natively map and constrain LLM boundaries per-domain without redeploying backend Go binaries. The LLM must "learn" the platform syntaxes directly (and solely) through semantic retrieval context injection.

### 6. Dynamic Context Injection and Intent Classification
To manage token bandwidth and prevent instruction contamination, SOP uses a two-stage architecture: classification first, deterministic expansion second.

Stage A: Three-Gate classification

- Gate 1 (Focused Prefix): parse explicit constraints and classify only missing layers and CRUD intent.
- Gate 2 (MRU Continuity/Switch): validate continuation vs switch and update layers/CRUD.
- Gate 3 (Cold Start Discovery): classify from a bounded context outline of entities/domains/artifacts.

Stage B: Focused context expansion

- Classification remains coarse by design (entity/domain/db_artifacts/layers).
- A deterministic assembler expands classification into execution-ready prompt context.
- Expansion is scoped by domain, artifact, CRUD, and relation metadata.
- The expanded payload is injected as a dedicated prompt component rather than broad domain dumps.

Architectural rule:

- Classification decides what is relevant.
- Expansion fetches the exact context.
- Prompt builder injects only the expanded focused context.

This eliminates prompt bloat and minimizes structural hallucinations while preserving deterministic routing behavior.

### 7. Progressive Native ReAct Loop

Once routing and focused context assembly finish, the native ReAct engine takes over as the inner micro loop.

System split:

- Macro loop: Gate 1, Gate 2, and Gate 3 prepare the Ask frame.
- Micro loop: the native reasoning engine executes bounded tool-based progression inside that frame.

This separation is intentional. Routing gates do not run on every retry. They shape the Ask once, then the inner loop progresses from actual execution evidence.

Core loop model:

- The inner loop's source of truth is the ordered history of tool attempts and tool results for the current Ask.
- After each step, the engine compacts that history into an Ask-anchored MRU summary.
- The next LLM call sees a compact current truth rather than a replay of the original broad prompt.

The Ask-anchored MRU carries the execution delta that matters most:

- current focus
- preserved valid work
- grounded confirmed facts
- what is still missing
- suggested next tools
- latest repair requirement when applicable

Structured tool envelopes:

- Tools can return a user-visible `tool_result` together with an internal `progress_hint`.
- `progress_hint` communicates convergence evidence such as status, completion delta, clues, missing fields, and suggested next tools.
- The engine consumes those hints; tools do not mutate MRU or scratchpad state directly.

Retry architecture:

- Recoverable failures are converted into constrained repair paths rather than immediate hard failure.
- Malformed native tool calls trigger a repair retry that demands exactly one valid tool call.
- Schema or relation uncertainty triggers research-first retry, typically `list_stores -> execute_script`.
- Same-tool argument defects trigger in-place repair: preserve valid arguments and rewrite only the broken slice.
- If the model tries to switch to an unrelated tool while repair is pending, the engine blocks the diversion and reasserts the allowed repair path.

Progressive visibility for the model:

- Retry prompts expose concrete prior tool arguments together with system tool responses.
- The prompt also carries the latest failure detail and the most recent successful hint context.
- The prompt now also includes an explicit `Progression history` array so the model can inspect each prior step as:
    - `ingredients`
    - `generated_call`
    - `result`
    - `progression`
- `progression` carries live convergence metadata such as status, completion delta, tips, clues, missing fields, suggested next tools, and retry instruction when present.
- This allows the model to refine the next internal ask or tool call from what improved, what remains missing, and which script slices should be preserved.

Bounded progression:

- The loop starts with a small retry budget.
- It extends only when grounded progress is detected.
- Grounded progress currently means at least one of:
    - new confirmed facts
    - a new positive `progress_hint`
    - a newly learned implicit repair recipe
- This keeps the loop progressive without allowing indefinite wandering.

Recipe interplay:

- When a repair path succeeds, the engine distills that successful sequence into an implicit micro recipe.
- Those learned recipes are persisted for later asks, but they also matter inside the current Ask.
- A newly learned recipe counts as live progress, so the retry controller can grant one more bounded step even without a fresh hint on that exact iteration.

Hard-stop semantics:

- Tools can explicitly report terminal negative states such as `blocked`, `anti_success`, `hard_error`, or `terminal_error`.
- Those statuses short-circuit the inner loop immediately.
- User-meaningful terminal outcomes are returned as tool results; infrastructure failures remain typed backend errors.

Architectural result:

- The macro loop decides the initial frame.
- The micro loop advances from grounded evidence.
- MRU compaction makes retries progressive.
- structured hints and learned recipes steer bounded convergence.
- terminal signals prevent pointless retry spirals.

Execution sequence:

1. User Ask enters Gate 1 / Gate 2 / Gate 3.
2. Routing and deterministic expansion build the Ask frame.
3. Native ReAct iteration 1 chooses the best next tool from that frame.
4. Tool result returns user payload plus optional `progress_hint`.
5. Engine classifies the step as progress, repair-required, stall, or terminal stop.
6. Engine compacts ordered tool history into Ask-anchored MRU.
7. Next LLM call sees preserved valid args, grounded facts, missing clues, and repair delta.
8. Loop either narrows again, enters research-first repair, repairs in place, or exits with final answer.
9. Successful repair paths are distilled into implicit recipes and persisted as later continuity signals.

Worked example: `Find John > 500`

This is the canonical Stores path that motivated the current progressive loop design.

1. Initial Ask frame.

- The user asks for users named John with orders greater than 500.
- Routing resolves this as Stores-focused read behavior.
- Gate 3 contributes compact Stores protocol context and relevant recipe slices.

2. First tool attempt is plausible but under-grounded.

- The model emits an `execute_script` call before relations or exact field mappings are fully grounded.
- The backend validates the call and recognizes a recoverable schema or join-shape defect.
- Instead of hard failing, the engine converts the failure into a repair-required result with explicit retry guidance.

3. The retry frame becomes narrower.

- The next prompt does not replay the whole manual.
- It carries:
    - repair directive
    - the failing tool result
    - the attempted script shape
    - Ask-anchored MRU telling the model to preserve valid work and change only the broken slice
- If the defect indicates missing schema or relation grounding, the engine forces research-first behavior.

4. Research step grounds the query.

- The next valid step becomes `list_stores`.
- `list_stores` returns schema and relation facts such as the relevant stores and join mapping surface.
- Its structured `progress_hint` tells the engine that the Ask is progressing and suggests the next tool.

5. Ask-anchored MRU compacts the new truth.

- The loop now carries grounded stores, relations, suggested next tool, and any remaining missing pieces.
- The prompt also preserves the previous valid script slices rather than discarding them.

6. Repaired execution reuses what was already correct.

- The model retries `execute_script` with corrected grounded arguments.
- The repaired call uses the researched schema and relations instead of guessing.
- The loop avoids broadening scope because the repair path is explicitly constrained.

7. Success produces both an answer and a learned pattern.

- The corrected query succeeds and returns the user-visible result.
- The engine recognizes the successful `execute_script` failure -> `list_stores` -> `execute_script` sequence.
- That sequence becomes an implicit micro recipe: research grounded schema before retrying `execute_script`.

8. The retry controller learns during execution, not only after it.

- That newly proven recipe counts as progress for the current Ask.
- If one more bounded step were needed, the controller could extend the retry budget based on that learned pattern even without a fresh hint on the same step.

Why this example matters:

- The loop does not treat retries as identical retries of the same broad prompt.
- Failure detail, returned context, concrete script args, missing signals, and grounded research all become input to the next micro step.
- The result is a loop that progressively narrows the problem until the model can either answer safely or stop explicitly.

Sequence sketch:

```mermaid
sequenceDiagram
    participant User
    participant Gates as Gate 1 / 2 / 3
    participant Prompt as Ask Frame
    participant Engine as Native ReAct Engine
    participant Tools as Local Tools
    participant MRU as Ask-Anchored MRU
    participant STM as Session Continuity

    User->>Gates: Ask
    Gates->>Prompt: routed frame + focused context + recipes
    Prompt->>Engine: start inner loop
    Engine->>Tools: tool call
    Tools-->>Engine: tool_result + progress_hint
    Engine->>MRU: compact progress / repair / missing / preserve
    MRU-->>Engine: current truth for next iteration
    Engine->>Tools: narrowed retry or research-first step
    Tools-->>Engine: grounded success
    Engine->>STM: persist Ask outcome + learned recipes
    Engine-->>User: final answer
```

Failure and progression taxonomy:

| Loop signal | Meaning | Engine response |
| :--- | :--- | :--- |
| `progress` | The Ask became more grounded through facts, hints, or a proven repair pattern. | Preserve valid work, compact the new delta into Ask MRU, and allow another bounded step. |
| `repair_required` | The last step failed in a recoverable way. | Constrain the next iteration to same-tool repair or research-first repair instead of broad replanning. |
| `stall` | A step completed but produced no new facts, no positive hint, and no new recipe. | Do not extend the retry budget; continue only within the remaining cap. |
| `success` | The model can answer from accumulated grounded state without another tool. | Exit the loop with a final answer or structured tool result. |
| `terminal_error` | The tool reported a meaningful user-facing terminal stop. | Short-circuit the loop and surface the tool result directly. |
| `hard_error` / `blocked` / `anti_success` | The tool explicitly signaled that continuing would be counterproductive. | Stop immediately and do not spend more retry budget. |
| non-recoverable backend error | Infrastructure or orchestration failure, not a user-facing tool outcome. | Return a typed backend error rather than converting it into loop-local retry guidance. |

### 8. Macro Data Flow (Outer Multi-Ask Loop)

The micro loop explains how one Ask converges. The macro data flow explains how one Ask becomes the starting continuity surface for the next Ask.

Primary macro artifacts:

- user Ask
- routing state
- Ask frame
- Ask outcome
- session MRU projection
- recipe snapshot
- next Ask frame

Macro flow:

1. User Ask arrives.

- The query enters the routing layer with any available session continuity.
- Session MRU provides recent successful patterns, active domains, prior Ask outcomes, and follow-up hints.

2. Routing resolves the Ask entry point.

- Gate 1 contributes explicit anchors when the user names a focused path.
- Gate 2 decides continuation versus topic switch from continuity digest plus fresh anchor evidence.
- Gate 3 completes cold-start discovery and recipe-oriented prompt handoff when prior continuity is insufficient.

3. Prompt assembly builds the Ask frame.

- The Ask frame is assembled from persona, focused tools, playbooks, recipes, focused execution context, and user query.
- This is the macro loop's main output into the micro loop.

4. Native ReAct engine executes the Ask.

- The inner loop performs repair, research, retries, and final answer synthesis inside the Ask frame.
- During this phase, Ask-anchored MRU is local to the current Ask only.

5. Ask finishes and emits an Ask outcome.

- The Ask outcome is the compact bridge object from one Ask into later continuity.
- It contains the user-visible result plus the most useful compacted execution signals, such as:
    - tools used
    - confirmed facts
    - successful repair or research pattern
    - failure summary when the Ask ended imperfectly
    - learned implicit recipes

6. Session continuity is updated.

- The Ask outcome is projected into session MRU.
- Learned implicit recipes are merged into the bounded recipe snapshot.
- Existing STM episode logging remains orthogonal and does not drive the active macro loop yet.

7. Next Ask starts from delta, not replay.

- A follow-up Ask does not need the full previous prompt replayed.
- The macro loop re-enters from compact continuity: recent Ask outcome, active domain, prior successful pattern, and remaining uncertainty.
- This is what allows follow-ups such as "same query but > 1000" to behave as continuations instead of cold starts.

Macro design rule:

- The macro loop carries continuity between Asks.
- The micro loop carries convergence within one Ask.
- Ask-anchored MRU must not leak upward as raw inner-loop noise.
- Session MRU should carry only compacted outcomes and reusable patterns that improve the next Ask.

Macro sequence sketch:

```mermaid
sequenceDiagram
    participant User
    participant Session as Session MRU
    participant Gates as Gate 1 / 2 / 3
    participant Prompt as Ask Frame Builder
    participant Engine as Native ReAct Loop
    participant Outcome as Ask Outcome
    participant Recipes as Recipe Snapshot

    User->>Session: new Ask with possible follow-up intent
    Session->>Gates: continuity digest + recent patterns
    Gates->>Prompt: routed scope + focused context handoff
    Prompt->>Engine: Ask frame
    Engine->>Outcome: final result + compacted facts + learned recipes
    Outcome->>Session: update MRU continuity
    Outcome->>Recipes: merge bounded implicit recipes
    Session-->>User: next Ask starts from compact continuity
```

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



---

## Agentic Routing and Prompt Assembly
The active OMNI routing and prompt path is intentionally explicit:

1. Route with Three-Gate classification.
2. Persist routing state for continuity.
3. Build focused execution context from classification.
4. Inject focused execution context as a dedicated prompt component.
5. Fall back to broad schema injection only when no specific artifacts are classified.

This design keeps routing deterministic while giving the LLM laser-focused execution context for fluent script generation.
