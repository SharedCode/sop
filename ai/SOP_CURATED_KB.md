# Platform Foundations

This category captures the core architectural model, platform abstractions, and design assumptions that define how SOP works.

## Architecture

This category explains the backend topology, registry-centered consistency model, and storage layouts that underpin SOP deployments.

- Item: Filesystem and hybrid backend model
  Summary: SOP supports a filesystem backend and a hybrid Cassandra-backed metadata backend.
  Summary: The filesystem backend stores registry metadata in a filesystem-native mapped structure.
  Summary: The hybrid backend stores registry metadata in Cassandra while keeping blobs on the filesystem.
  Summary: Both backends share the same transactional model and differ mainly in registry placement and operational tradeoffs.
  Body:
  SOP exposes two primary backend models. The recommended `infs` backend stores both metadata and data on the filesystem, using Redis only for distributed locking and caching. The `incfs` backend stores registry metadata in Cassandra while keeping B-Tree nodes and data values in the filesystem blob store. In both cases, the transaction model is the same: writes prepare data first and then make them durable by updating the registry layer.
  Sources: ARCHITECTURE.md, README.md

- Item: Registry as source of truth
  Summary: SOP uses the registry as the authoritative mapping from logical IDs to physical locations.
  Summary: Registry updates define the commit point for copy-on-write object replacement.
  Summary: Local cache staleness is avoided by looking up current physical handles through the registry first.
  Body:
  SOP identifies nodes and items through logical IDs and uses the registry to map them to current physical locations. This allows copy-on-write updates without breaking references. During reads, clients consult the registry before relying on local cache entries, which avoids returning stale values from older physical versions. During commit, the registry update is the durable transition from prepared artifacts to live state.
  Sources: ARCHITECTURE.md

## Core Concepts

This category defines the main platform concepts that separate structured persistence, semantic memory, and shared runtime surfaces.

- Item: Stores and Spaces as separate domains
  Summary: SOP separates deterministic Stores from semantic Spaces.
  Summary: Stores are for structured B-Tree persistence and Spaces are for knowledge-oriented semantic memory.
  Summary: Both domains can coexist in the same product while serving different operational needs.
  Body:
  Stores are the general-purpose persistence surface for key-value data, structured records, joins, CRUD operations, and transaction-oriented workflows. Spaces are the semantic memory surface used for knowledge categories, items, vectorization, and retrieval. This distinction keeps structured persistence and semantic retrieval aligned without forcing them into one abstraction.
  Sources: README.md, WORKFLOWS.md, ai/README.md

- Item: System database role
  Summary: Every SOP environment includes a SystemDB for platform metadata and runtime artifacts.
  Summary: SystemDB stores scripts, knowledge, and other internal state used by the runtime.
  Summary: User databases and semantic spaces are separate from this shared system layer.
  Body:
  SOP provisions a built-in SystemDB in each environment. It stores internal metadata and runtime artifacts such as automation scripts, operational knowledge, and other system-level records. This keeps shared runtime state separate from application data stores and semantic spaces while preserving the same transactional model across the platform.
  Sources: README.md, ARCHITECTURE.md

## Design Principles

This category records the guiding design choices that shape SOP APIs, retrieval structure, and backend encapsulation.

- Item: Bounded semantic topology over raw heading structure
  Summary: The curated KB should use a stable semantic taxonomy instead of raw document headings.
  Summary: Category hierarchy should be bounded, reusable, and intentionally designed for retrieval.
  Summary: Headings define semantic topology while item summaries define vector indexing payload.
  Body:
  The curated knowledge source should not mirror arbitrary repository heading trees. Instead, it should define a bounded semantic hierarchy that is intentionally designed for retrieval. The heading structure determines the category graph, and the explicit item summaries under those headings provide the vector-facing indexing surface. This allows prompts to target meaningful categories and then match items through concise semantic summaries.
  Sources: ai/KB_CURATION_MANIFEST.md, ai/cmd/knowledge_compiler/README.md

- Item: Encapsulation and backend abstraction
  Summary: SOP keeps complex storage details behind high-level public packages.
  Summary: Internal packages hold lower-level backend logic that should not leak into consumer contracts.
  Summary: Public features are exposed through stable packages such as `infs`, `incfs`, and higher-level database APIs.
  Body:
  SOP separates public APIs from internal storage mechanics so consumers can rely on stable higher-level packages without coupling to low-level blob or registry details. This is especially important for the hybrid backend, where internal Cassandra/blob mechanics remain encapsulated while public packages expose transactional behavior through consistent APIs.
  Sources: ARCHITECTURE.md

## Data Model & Abstractions

This category covers the abstractions that expose SOP data structures, schemas, and high-level database interfaces.

- Item: Metadata-carrying keys
  Summary: SOP supports complex B-Tree keys that carry state and routing metadata.
  Summary: Key fields can hold version, status, deletion, category, or vector-routing metadata.
  Summary: This allows many structural operations to run without loading the heavier value payload.
  Body:
  SOP does not treat keys as opaque strings only. Keys can be structured objects that carry state such as version, deleted flags, routing identifiers, or category information. This enables scans and filters to operate on key material directly, which reduces value fetches for common structural tasks and is used in both general data stores and vector-oriented paths.
  Sources: README.md, ARCHITECTURE.md, ai/README.md

- Item: Unified database abstraction
  Summary: SOP provides a high-level database abstraction over stores, vectors, search, and models.
  Summary: Applications can use one entry point to open different persistence surfaces while sharing transactions.
  Summary: This abstraction supports both standalone and clustered deployments.
  Body:
  The database abstraction gives applications a single entry point for opening B-Trees, vector stores, search indexes, and model stores. It reduces configuration duplication and allows different persistence surfaces to participate in the same transactional flow where appropriate.
  Sources: README.md, GO_CORE_ENGINE.md, ai/README.md

- Item: Flat table abstraction over key-value storage
  Summary: SOP exposes stores to AI and application layers as flat table-like schemas rather than raw key and value wrappers.
  Summary: `key_fields` and `value_fields` identify structure without forcing nested Key or Value prefixes into queries.
  Summary: Relations and predicates operate on schema field names directly.
  Body:
  Internally, SOP stores data as structured key-value pairs in B-Trees, but the higher-level schema model removes those wrappers and presents a table abstraction. Fields appear at one flat level, primary-key components are identified explicitly, and relation mappings use schema field names directly. This keeps storage efficient while making query planning, joins, and AI-facing tooling easier to reason about.
  Sources: TABLE_ABSTRACTION.md, README.md

# Installation & Setup

This category covers the environment requirements, configuration model, and setup flows needed to run SOP.

## Prerequisites

This category summarizes the runtime dependencies and platform requirements needed before installing or deploying SOP.

- Item: Core runtime prerequisites
  Summary: Go is the primary prerequisite for native SOP development.
  Summary: Redis is optional for standalone use and required for clustered coordination.
  Summary: Cassandra is only required when the hybrid backend is selected.
  Body:
  Native development with SOP requires Go. Redis is not required for standalone deployments using in-memory coordination, but clustered deployments depend on Redis for lock coordination and caching. Cassandra is only needed when the `incfs` backend is used for metadata storage.
  Sources: GETTING_STARTED.md, OPERATIONS.md, ARCHITECTURE.md

## Configuration

This category describes the configuration knobs that control storage layout, redundancy, caching, and performance behavior.

- Item: Storage redundancy configuration split
  Summary: SOP configures registry redundancy separately from user-data striping.
  Summary: `StoresFolders` controls registry and system-table redundancy.
  Summary: `ErasureConfigs` controls striping and parity for user data files.
  Body:
  SOP separates system-critical metadata configuration from bulk data configuration. `StoresFolders` define the registry and system-table partitions, usually with active/passive failover. `ErasureConfigs` define how user data is striped and protected through Reed-Solomon parity. This split lets operators choose different durability and performance strategies for metadata and bulk content.
  Sources: CONFIGURATION.md, README.md

- Item: Cache and persistence tuning
  Summary: SOP exposes configuration for cache layers, blob placement, and node density.
  Summary: `SlotLength`, cache TTLs, and active persistence settings influence throughput and memory usage.
  Summary: Large-object workloads are expected to keep the B-Tree lean and push values into blobs.
  Body:
  Configuration and tuning revolve around balancing node density, cache reuse, and blob storage behavior. `SlotLength` influences B-Tree node density. Cache factory and store cache settings control TTL and backend selection. Large-object workloads typically use active persistence and blob storage so commits stay smaller and index structures remain efficient.
  Sources: CONFIGURATION.md

## Environment Setup

This category covers first-run provisioning, setup wizards, and language-specific installation paths for getting started.

- Item: Data Manager setup wizard
  Summary: The Data Manager setup wizard initializes deployment mode, storage paths, and optional demo data.
  Summary: Standalone mode uses local folders while clustered mode adds Redis-backed coordination.
  Summary: First-run setup creates the system database and an initial user database.
  Body:
  After launching the SOP HTTP Server, users interact with a web-based Setup Wizard to choose Standalone or Clustered mode, configure storage folders, optionally define erasure-coding settings, and preload demo data. The wizard provisions the SystemDB and one or more user databases so applications and the Data Manager can start operating immediately.
  Sources: docs/installation/DATA_MANAGER_SETUP.md, GETTING_STARTED.md

- Item: Language-specific installation paths
  Summary: Go integrates directly through modules, while other bindings use packaged artifacts or platform bundles.
  Summary: Python uses pip, C# uses NuGet, Java uses JAR-based distribution, and Rust uses Cargo and FFI bindings.
  Summary: Platform bundles include the Data Manager binary and language-specific assets for non-Go workflows.
  Body:
  SOP supports several installation paths depending on language. Go developers import the module directly. Python, C#, Java, and Rust developers consume packaged artifacts or platform bundles that include shared libraries and related tooling. This keeps the runtime consistent while matching the expected packaging model of each language ecosystem.
  Sources: docs/installation/LANGUAGE_BINDINGS.md, GETTING_STARTED.md, bindings/python/README.md, bindings/csharp/README.md, bindings/java/README.md, bindings/rust/README.md

## Deployment Modes

This category explains the supported deployment topologies and how standalone and clustered modes differ operationally.

- Item: Standalone and clustered deployment modes
  Summary: SOP supports standalone and clustered deployment modes.
  Summary: Standalone mode can use in-memory coordination without Redis.
  Summary: Clustered mode uses Redis-backed coordination and shared storage paths.
  Body:
  Standalone mode is intended for local, embedded, or single-process deployments and can use in-memory locking and caching. Clustered mode relies on Redis for coordination and usually pairs with shared or mounted storage. Both modes use the same storage and transaction concepts, but differ in coordination and deployment topology.
  Sources: ARCHITECTURE.md, GETTING_STARTED.md, WORKFLOWS.md

# Storage & Data Management

This category describes how SOP stores, coordinates, and serves structured and semantic data across its persistence surfaces.

## Stores vs Spaces

This category distinguishes the structured store model from the semantic space model so each is used for the right workload.

- Item: Stores for structure, Spaces for semantics
  Summary: Stores are optimized for structured persistence and query workflows.
  Summary: Spaces are optimized for category-oriented semantic memory and retrieval.
  Summary: The split helps preserve strong transactional workflows without diluting semantic retrieval design.
  Body:
  Stores are the deterministic data-management path for B-Trees, structured records, joins, CRUD operations, and transactional logic. Spaces are the semantic path for knowledge categories, items, vectorization, and memory-oriented retrieval. Both remain first-class, but they serve different workloads and should not be treated as interchangeable abstractions.
  Sources: README.md, ai/README.md, WORKFLOWS.md, ai/agent/STORES_VS_SPACES.md

## Transactions

This category explains the commit model and consistency guarantees that let multiple SOP surfaces share one transactional foundation.

- Item: Commit-through-registry transaction model
  Summary: SOP uses a two-phase commit model built around prepared artifacts and registry updates.
  Summary: Prepared blobs become live only when registry handles are updated.
  Summary: This allows copy-on-write updates with ACID isolation across the storage model.
  Body:
  SOP writes modified nodes and values as prepared artifacts first. A transaction becomes durable only when the registry is updated to point at the new physical locations. This commit-through-registry model supports copy-on-write updates, rollback safety, and consistent reads without relying on a central database server process.
  Sources: ARCHITECTURE.md

- Item: Multi-surface transactional consistency
  Summary: SOP can coordinate structured stores, search, and some AI-facing persistence under one transactional model.
  Summary: The system prefers one storage engine foundation across different data-management surfaces.
  Summary: This reduces divergence between operational data and AI-oriented persistence flows.
  Body:
  One of SOP's design goals is to keep different persistence features on a common transactional foundation. Structured stores, text search, model storage, and AI-facing layers all build on the same B-Tree and registry concepts. This allows operational and AI-related workloads to share consistency guarantees and minimizes the need for separate database products.
  Sources: README.md, ai/README.md, GO_CORE_ENGINE.md

## Caching & Consistency

This category covers how SOP balances cache performance with registry-based consistency and coordination.

- Item: Registry-first consistency model
  Summary: SOP uses registry lookups before trusting local caches.
  Summary: Redis is used for distributed locking and cache coordination rather than as the primary data store.
  Summary: The design avoids stale reads by resolving the current physical handle before value access.
  Body:
  SOP employs a multi-tier caching model, but consistency is anchored on the registry rather than blindly trusting local cache state. Clients resolve the current physical handle from the registry before using local data, which prevents stale reads after copy-on-write updates. Redis supplies distributed coordination and caching behavior, not the primary data persistence layer.
  Sources: ARCHITECTURE.md, OPERATIONS.md

## Backend Options

This category compares the primary filesystem backend with the hybrid Cassandra-backed metadata option.

- Item: Filesystem backend as the primary deployment model
  Summary: The filesystem backend is the primary and recommended backend for most deployments.
  Summary: It stores data and registry structures on disk and uses Redis only for clustering concerns.
  Summary: It has outperformed the hybrid backend in the referenced repo stress tests.
  Body:
  The `infs` backend is the main storage model used across SOP. It keeps both registry and blob data on the filesystem and layers Redis coordination on top when clustering is required. This model keeps the operational stack smaller and, in the documented tests, performed better than the hybrid backend while retaining the same transactional behavior.
  Sources: ARCHITECTURE.md, WORKFLOWS.md

- Item: Hybrid Cassandra backend for metadata replication
  Summary: The hybrid backend stores registry metadata in Cassandra while keeping blobs on the filesystem.
  Summary: It is appropriate when Cassandra infrastructure already exists or when metadata replication should follow Cassandra policies.
  Summary: Redis still supplies locking and caching for clustered coordination.
  Body:
  The `incfs` backend splits metadata and blob storage. Cassandra stores registry and root metadata, while B-Tree nodes and data blobs remain on the filesystem. This allows teams that already operate Cassandra clusters to reuse that infrastructure for metadata while keeping bulk data on disk and preserving SOP's transactional model.
  Sources: ARCHITECTURE.md, WORKFLOWS.md, CONFIGURATION.md

## Filesystem / Redis / Cassandra

This category clarifies the distinct operational roles of filesystem storage, Redis coordination, and Cassandra metadata services.

- Item: Backend role separation
  Summary: Filesystem stores blobs and registry data in the main backend.
  Summary: Redis provides coordination and caching rather than primary persistence.
  Summary: Cassandra is only part of the hybrid backend and is used for registry metadata.
  Body:
  In the recommended backend, the filesystem stores both registry and blob data, while Redis handles coordination and cache distribution. In the hybrid backend, Cassandra replaces the filesystem registry layer for metadata, while blobs remain on disk. This separation keeps operational roles clear across the backend stack.
  Sources: ARCHITECTURE.md, CONFIGURATION.md, OPERATIONS.md

# Operations & Reliability

This category covers the testing, failure handling, tuning, observability, and security practices needed to operate SOP safely.

## Testing

This category summarizes how SOP validates behavior through unit, integration, stress, and reproducible Docker-based test flows.

- Item: Unit, integration, and stress test separation
  Summary: SOP test suites are separated into unit, integration, and stress scopes.
  Summary: Unit tests validate core behavior without external services.
  Summary: Integration and stress runs use explicit build tags and longer-running environments.
  Body:
  SOP uses default unit tests for fast local validation, while integration and stress scenarios are separated using build tags and specific task runners. This lets developers run lightweight validation during normal development and reserve Redis-, Cassandra-, or concurrency-heavy scenarios for integration environments and pre-release checks.
  Sources: README.md, WORKFLOWS.md, DEV_TOOLS.md

- Item: Docker-based reproducible test environments
  Summary: Docker-based test flows provide reproducible dependency and environment setup.
  Summary: Separate Docker files allow normal and no-coverage test runs.
  Summary: Root test scripts orchestrate broader multi-language or combined runs.
  Body:
  Docker-based testing helps keep environments reproducible across local and CI systems. The repository includes standard and no-coverage Docker flows along with shell scripts that orchestrate broader test suites. This reduces variation in dependency versions and runtime environment when validating the storage engine and related tooling.
  Sources: README.md, DEV_TOOLS.md

## Failover & Recovery

This category explains how SOP handles failover conditions, backup coordination, and recovery boundaries for durable storage.

- Item: Failover-qualified I/O errors
  Summary: SOP distinguishes transient errors from failover-qualified storage errors.
  Summary: Qualified errors include I/O failure, read-only filesystems, and no-space conditions.
  Summary: On qualified write failure, SOP switches to the configured standby path.
  Body:
  SOP does not treat every error as a failover event. Instead, it identifies specific storage failures such as `EIO`, `EROFS`, and `ENOSPC` as failover-qualified. When such an error occurs on a write path, the system marks the current storage path passive and continues on the configured active standby path. The failed path then requires intervention or repair.
  Sources: OPERATIONS.md

- Item: Backup and restore coordination
  Summary: Hybrid backup requires coordinated snapshots of Cassandra metadata and filesystem blobs.
  Summary: Consistent restore depends on restoring both registry state and blob content together.
  Summary: Copy-on-write behavior helps preserve point-in-time restore safety.
  Body:
  For the hybrid backend, operators snapshot Cassandra keyspace state and the blob filesystem together. If a zero-downtime snapshot is needed, the registry snapshot should be taken first and the filesystem snapshot second. Because SOP uses copy-on-write semantics, old blobs referenced by the older registry snapshot remain available long enough to preserve a consistent restore point.
  Sources: OPERATIONS.md

## Performance

This category describes the main throughput and scalability levers that affect SOP behavior in production.

- Item: Slot length as a density and throughput control
  Summary: `SlotLength` controls how many items live in a B-Tree node.
  Summary: Higher slot lengths increase density and can improve throughput for large datasets.
  Summary: Tradeoffs include node size, memory footprint, and workload-specific tuning.
  Body:
  `SlotLength` is one of the main B-Tree tuning levers in SOP. Larger values increase node density and reduce the number of traversed nodes, which can improve throughput for large datasets. The tradeoff is larger node size and potentially higher memory pressure. The configuration guide recommends tuning slot length by workload rather than treating one value as universal.
  Sources: CONFIGURATION.md, README.md, SCALABILITY.md

- Item: Scalability bounded by deployment design and hardware
  Summary: SOP scale is constrained primarily by storage, bandwidth, and coordination design.
  Summary: Registry partitioning and file layout are used to extend addressable capacity.
  Summary: Horizontal scaling depends on backend throughput, network capacity, and operational tuning.
  Body:
  SOP is designed so the practical scaling limit comes from hardware capacity and deployment architecture rather than from a central database server. Registry partitioning, segmented storage, and distributed coordination allow the system to address very large datasets, but actual throughput still depends on storage IOPS, network fabric, and how the cluster is tuned.
  Sources: SCALABILITY.md, CONFIGURATION.md

## Monitoring & Debugging

This category covers the logs, inspection surfaces, and operational signals used to debug and observe SOP systems.

- Item: Operational observability basics
  Summary: SOP operations expose failover signals through logs and metrics.
  Summary: Operators should watch failover-tagged logs and any instrumented failover counters.
  Summary: The web UI can also be used to inspect stores and verify live data shape.
  Body:
  At the operational level, SOP expects teams to observe failover events through logs and optionally through instrumented metrics such as failover counters. The web-based Data Manager also serves as a practical inspection tool for verifying data integrity, schema interpretation, and live store contents during debugging.
  Sources: OPERATIONS.md, README.md

## Security & Access

This category describes the authentication, authorization, and entitlement controls that protect SOP management and data surfaces.

- Item: REST authentication modes for the Data Manager
  Summary: The HTTP server supports an internal mode and a protected public mode.
  Summary: Public mode requires REST authentication and root password configuration.
  Summary: Bearer-token enforcement protects script execution and other sensitive endpoints.
  Body:
  The SOP HTTP server can run in a trusted internal mode or in a public or edge-facing mode with authentication enabled. In protected mode, API requests must include valid bearer tokens, and root-level configuration controls are guarded separately. This allows the same management surface to serve both internal tooling and external automation workflows safely.
  Sources: tools/httpserver/SECURITY.md, OPERATIONS.md

- Item: Role-based access and entitlements
  Summary: SOP models access control through capabilities, limits, and resource-scoped rules.
  Summary: Entitlements can constrain compute-heavy actions and storage footprint.
  Summary: Global roles can be combined with local resource rules for finer control.
  Body:
  SOP's RBAC model is intended to be data-driven rather than hardcoded into application logic. Roles can define allowed operations, rate or quota limits, and storage constraints, while resources can apply local access rules on top. This enables multi-tenant deployments and differentiated service tiers without forcing policy logic into application binaries.
  Sources: RBAC_ENTITLEMENTS.md, OPERATIONS.md

# AI & Knowledge Systems

This category covers SOP's AI architecture, embedding paths, vector systems, memory model, and knowledge-base toolchain.

## Embedders

This category describes the hosted and local embedding paths that generate retrieval vectors for SOP knowledge systems.

- Item: Gemini embedding contract
  Summary: Gemini embedder supports `gemini-embedding-2` through `batchEmbedContents`.
  Summary: Requests set `taskType` to `RETRIEVAL_DOCUMENT`.
  Summary: Requests set `outputDimensionality` to `768`.
  Summary: SOP normalizes Gemini model names into the `models/...` form before dispatch.
  Body:
  The Gemini embedder uses the Google Generative Language batch embedding API. SOP normalizes the model name to the `models/...` form, sends retrieval-oriented embedding requests, and fixes the dimensionality request to 768 for the current hosted path. This keeps the hosted embedder aligned with the rest of the vector stack's dimensional expectations.
  Sources: ai/README.md, ai/DYNAMIC_VECTOR_STORE_DESIGN.md, ai/GEMINI_OPTIMIZATIONS.md

- Item: Local Kelindar embedder profiles
  Summary: SOP supports local Kelindar GGUF embedders for Nomic and BGE-small models.
  Summary: Nomic uses asymmetric storage and query prefixes plus Matryoshka routing slices.
  Summary: BGE-small is the lighter 384-dimensional local option for shorter retrieval text.
  Body:
  The local embedding path supports profile-driven behavior through the Kelindar integration. `nomic-embed-text-v1.5-q8_0` is used for richer asymmetric document/query flows and category-routing slices. `bge-small-en-v1.5-q8_0` is the smaller local option for compact retrieval cases. Profile metadata determines dimensions, prefixes, and whether Matryoshka routing normalization applies.
  Sources: ai/README.md, ai/DYNAMIC_VECTOR_STORE_DESIGN.md

## Vector Stores

This category covers the maintained vector architecture, legacy compatibility path, and how semantic storage is organized.

- Item: Dynamic Vector Store as maintained architecture
  Summary: The Dynamic Vector Store is the maintained AI memory direction.
  Summary: It uses semantic anchors and category-driven structure instead of legacy K-Means clustering.
  Summary: Legacy vector content remains supported and can be imported into Spaces.
  Body:
  The Dynamic Vector Store is the maintained vector architecture for AI memory and KnowledgeBase Studio. It uses semantic categories and anchor vectors rather than relying only on mathematical clustering. The predecessor vector store remains supported for backward compatibility, and legacy datasets can be imported into Memory Spaces, but new embedding, routing, and retrieval work should be documented against the dynamic store surfaces.
  Sources: ai/DYNAMIC_VECTOR_STORE_DESIGN.md, ai/README.md, ai/vector/VECTOR_STORE_DESIGN.md

- Item: Legacy vector store support and import path
  Summary: The predecessor vector store remains supported for backward compatibility.
  Summary: Its content can be imported into the newer Memory Spaces model.
  Summary: The maintained design surface for new vector work is the Dynamic Vector Store.
  Body:
  SOP continues to support the earlier vector store implementation for compatibility and existing datasets. However, the maintained product direction is the Dynamic Vector Store and Memory Spaces model. Legacy datasets are not abandoned; they can be imported into Spaces through the newer import and curation workflows.
  Sources: ai/vector/VECTOR_STORE_DESIGN.md, ai/README.md

## Memory

This category explains how SOP models short-term, long-term, and portable knowledge memory for AI agents.

- Item: Dual memory model for AI agents
  Summary: SOP uses separate short-term and long-term memory layers for AI agents.
  Summary: Runtime memory is scoped to the active Knowledge Base and persona.
  Summary: Long-term memory persists reusable knowledge through ACID-backed storage.
  Body:
  The AI runtime keeps session-scoped state in short-term memory and stores reusable knowledge in a persistent long-term memory layer. Knowledge Bases can carry their own prompts, embedders, and tool restrictions, so memory remains scoped to the active domain. This allows the system to preserve continuity without collapsing distinct personas or domains into one global context.
  Sources: ai/README.md, ARCHITECTURE.md, ai/memory/ACTIVE_MEMORY_DESIGN.md

- Item: Knowledge bases as portable memory assets
  Summary: Knowledge bases can be exported, imported, and reused across environments.
  Summary: The memory system preserves category structure and semantic knowledge during migration.
  Summary: This makes spaces portable across databases and deployments.
  Body:
  The modern memory system is not only a retrieval layer; it also acts as a portable knowledge asset. Export and import operations preserve category structure, summaries, and payload data so a curated space can be moved between environments or provisioned into a new deployment without rebuilding the structure from scratch.
  Sources: ai/memory/ACTIVE_MEMORY_DESIGN.md, ai/KNOWLEDGE_BASE_STUDIO.md, ai/README.md

- Item: Agent memory separates expertise, STM, and LTM
  Summary: The agent framework separates transient session context from persistent knowledge systems.
  Summary: Expertise KBs teach the agent how to operate, while STM and LTM carry live and durable conversational state.
  Summary: This architecture keeps domain knowledge scoped without collapsing all behavior into one global prompt.
  Body:
  The current agent model distinguishes short-term session state from longer-lived knowledge and memory stores. Static expertise can live in domain-specific or system knowledge bases, while short-term memory tracks the active conversation and long-term memory captures reusable learned context. This makes it possible to preserve continuity, isolate environments and personas, and keep operational knowledge separate from per-user memory.
  Sources: ai/agent/README.md, ai/memory/ACTIVE_MEMORY_DESIGN.md, ai/README.md

## Semantic Retrieval

This category describes the routing, distance math, and semantic anchor patterns that guide retrieval behavior.

- Item: Normalized-vector distance optimization
  Summary: SOP normalizes routing and category vectors once for the local Matryoshka path.
  Summary: For normalized vectors, Euclidean distance is computed from the dot product.
  Summary: The optimized formula is `sqrt(2 * (1 - dot(a, b)))` for unit vectors.
  Summary: This keeps Euclidean ordering while reducing repeated norm work in the search loop.
  Body:
  The memory math layer now uses a faster normalized-vector distance path for routing and category comparisons. Local Matryoshka-capable vectors are normalized at slice or index time, and distance calculations then use the dot-product-based Euclidean equivalent for unit vectors. This preserves the intended distance semantics while reducing hot-loop computation during search.
  Sources: ai/README.md, ai/DYNAMIC_VECTOR_STORE_DESIGN.md

- Item: Semantic anchors and routing categories
  Summary: The Dynamic Vector Store organizes retrieval around semantic anchors rather than blind clustering.
  Summary: Categories act as meaningful routing surfaces before fine-grained matching happens inside them.
  Summary: This makes retrieval more targetable than raw document-level vector clouds.
  Body:
  The Dynamic Vector Store uses semantic anchors and category-level routing so the query first lands in the right conceptual neighborhood. Instead of relying only on mathematical clusters, the system maps content into semantically meaningful categories and then performs fine-grained matching within those domains. This makes retrieval easier to steer and easier to explain.
  Sources: ai/DYNAMIC_VECTOR_STORE_DESIGN.md, ai/memory/ACTIVE_MEMORY_DESIGN.md

## Knowledge Base / KB Compiler

This category covers the curated-markdown workflow and tooling that turns semantic documentation into importable knowledge assets.

- Item: Curated markdown as compiler source
  Summary: The KB compiler is moving from raw repo-heading crawling toward curated markdown input.
  Summary: The curated file defines bounded semantic headings and explicit item blocks.
  Summary: The compiler packages those items into canonical `ExportData / ExportItem` JSON.
  Body:
  The KB compiler remains part of the toolchain, but its role is shifting. Instead of inferring categories directly from arbitrary repository headings, it can now parse a curated markdown source that explicitly defines semantic category headings plus `Item / Summary / Body / Sources` blocks. The compiler then emits canonical knowledge JSON compatible with the existing import/export path, making the output easier to reason about and better aligned with retrieval quality.
  Sources: ai/KB_CURATION_MANIFEST.md, ai/cmd/knowledge_compiler/main.go, ai/cmd/knowledge_compiler/README.md

- Item: KnowledgeBase Studio as the forward management surface
  Summary: KnowledgeBase Studio is the maintained visual surface for building and refining spaces.
  Summary: It supports category management, item editing, import/export, and vectorization controls.
  Summary: The curated KB workflow should align with this maintained management model.
  Body:
  The forward management surface for semantic knowledge is KnowledgeBase Studio. It exposes categories, items, import/export flows, vectorization controls, and other space-level operations through the UI. The curated KB workflow fits this model because it produces structured semantic content that can be imported and maintained in the same space-oriented management path.
  Sources: ai/KNOWLEDGE_BASE_STUDIO.md, ai/README.md

- Item: Stores and Spaces have distinct API surfaces
  Summary: SOP separates structured Stores APIs from semantic Spaces APIs while keeping both on one transactional platform.
  Summary: Stores fit business data and joins, while Spaces fit embeddings, categories, and semantic retrieval.
  Summary: The platform's visual tooling is built around this distinction rather than flattening both models into one generic interface.
  Body:
  SOP deliberately exposes different operational surfaces for Stores and Spaces. Stores are the structured B-Tree path for transactional business data, joins, and bulk CRUD operations. Spaces are the semantic path for categories, items, embeddings, vectorization, and retrieval. The two can coexist in one application and even share transactional behavior, but they should not be treated as interchangeable abstractions.
  Sources: ai/agent/API_ARCHITECTURE.md, ai/agent/STORES_VS_SPACES.md, ai/README.md

# Language Bindings & Tooling

This category summarizes the language-specific integration surfaces and tooling that extend SOP beyond the Go core.

## Go

This category covers the native Go API surface, storage configuration patterns, and primary engine integration path.

- Item: Native Go B-Tree API
  Summary: Go developers use native generic B-Tree APIs for type-safe access.
  Summary: Transactions follow explicit begin, commit, and rollback patterns.
  Summary: Structured keys and values can be modeled directly in code without an ORM layer.
  Body:
  SOP's Go API exposes direct B-Tree access through generics, allowing applications to model keys and values as concrete Go types. Transactions are managed explicitly, and advanced patterns such as structured keys, erasure coding, and metadata-carrying keys are all available in the native API surface.
  Sources: GO_CORE_ENGINE.md, README.md, COOKBOOK.md

- Item: Go replication and erasure coding setup
  Summary: Go applications configure erasure coding through explicit config structures.
  Summary: Data and parity shards are mapped across multiple storage folders.
  Summary: Repair behavior can be enabled for corrupted shard recovery during reads.
  Body:
  In Go deployments, erasure coding is configured through explicit structures that define data shards, parity shards, and storage locations. This maps logical SOP storage onto physical stripe layouts and allows repair paths to reconstruct missing or corrupted shards when the configuration enables it.
  Sources: GO_CORE_ENGINE.md, CONFIGURATION.md

## Python

This category describes the Python binding, its interoperability model, and the example workflows provided for Python users.

- Item: Python binding overview
  Summary: `sop4py` exposes SOP through Python-friendly wrappers.
  Summary: Python applications can access B-Trees, transactions, vector search, and AI features.
  Summary: The binding keeps the Go storage engine underneath while exposing Python-oriented APIs.
  Body:
  The Python binding provides a Pythonic interface to the SOP storage engine while preserving the same transactional and data-management semantics used by the Go runtime. This includes access to structured stores, AI-oriented persistence, and supporting examples for common integration paths.
  Sources: bindings/python/README.md, bindings/python/COOKBOOK.md, docs/installation/LANGUAGE_BINDINGS.md

- Item: Python examples and interop patterns
  Summary: Python examples demonstrate CRUD, interop, and vector-search workflows.
  Summary: Interop-friendly serialization allows Python-created data to be consumed by other bindings.
  Summary: Examples also show AI-oriented usage for semantic retrieval scenarios.
  Body:
  Python examples in the binding package cover common storage patterns, interop behavior, and AI workflows. They show how Python can participate in the same shared SOP data model used by other language bindings while still supporting semantic retrieval and vectorized use cases.
  Sources: bindings/python/README.md, bindings/python/COOKBOOK.md, bindings/python/sop/examples/README.md

## C#

This category covers the .NET binding, packaged tooling, and automation-oriented usage patterns for C# applications.

- Item: C# binding overview
  Summary: The C# binding integrates SOP into .NET applications through typed APIs and packaged distribution.
  Summary: It supports transactional B-Trees, complex keys, and AI-oriented persistence patterns.
  Summary: CLI tooling is available alongside the runtime package.
  Body:
  The C# binding packages the SOP runtime for .NET applications and exposes strongly typed storage and transaction abstractions that mirror the core engine. Alongside the runtime package, CLI tooling is available for related management and example workflows.
  Sources: bindings/csharp/README.md, bindings/csharp/Sop.CLI/README.md, docs/installation/LANGUAGE_BINDINGS.md

- Item: C# cookbook and automation patterns
  Summary: C# documentation covers CRUD, indexing, transactions, and script-driven automation.
  Summary: Examples show how local .NET applications can integrate with SOP HTTP and script surfaces.
  Summary: The binding is positioned as a typed .NET entry point over the same storage model.
  Body:
  C# documentation includes common storage patterns, transactional usage, indexing examples, and interaction with server-side automation surfaces. This makes the binding suitable both for direct embedded storage and for applications that interact with SOP's HTTP and script endpoints.
  Sources: bindings/csharp/README.md, bindings/csharp/Sop.CLI/README.md

## Rust

This category describes the Rust integration layer and how it preserves idiomatic resource handling over the SOP runtime.

- Item: Rust binding overview
  Summary: The Rust binding wraps the SOP runtime through FFI while preserving Rust-friendly resource handling.
  Summary: Contexts, transactions, and data structures can be managed with idiomatic ownership patterns.
  Summary: Serialization support allows Rust types to participate in SOP storage cleanly.
  Body:
  The Rust binding provides Rust applications with access to the SOP runtime through FFI-backed abstractions that are designed to fit Rust's ownership and cleanup model. This allows Rust services and tools to participate in SOP's transactional storage and AI-related persistence surfaces without abandoning idiomatic resource management.
  Sources: bindings/rust/README.md

## Java

This category covers the JVM integration path, Java runtime wrappers, and Java-oriented workflow guidance.

- Item: Java binding overview
  Summary: The Java binding exposes the SOP runtime through native access wrappers and typed APIs.
  Summary: Java applications can use B-Trees, transactions, and script-oriented HTTP integration.
  Summary: Packaging is aligned with common JVM distribution expectations.
  Body:
  The Java binding gives JVM applications access to SOP's storage engine using a native-access integration layer. The binding documents typed B-Tree usage, transaction patterns, and HTTP-based integration with script execution and management workflows.
  Sources: bindings/java/README.md, bindings/java/TUTORIAL.md, docs/installation/LANGUAGE_BINDINGS.md

## CLI / Examples

This category summarizes the runnable examples, bundled entry points, and operational tooling that help teams exercise SOP.

- Item: Examples as interop templates
  Summary: The examples directory demonstrates end-to-end SOP usage patterns.
  Summary: Interop examples show data written in one language being consumed by others.
  Summary: These examples are intended as templates for new projects and testing flows.
  Body:
  The repository's examples are both runnable demonstrations and integration templates. They show how different languages and access patterns map onto the same storage model and help teams validate how stores, indexes, and transactions behave in practice.
  Sources: examples/README.md, examples/interop_indexes/README.md

- Item: Data Manager and bundled CLI entry points
  Summary: Platform bundles include `sop-httpserver` for launching the Data Manager.
  Summary: Language-specific packages and tooling expose additional CLI-oriented entry points.
  Summary: Shell scripts orchestrate broader suite and example execution in development and CI.
  Body:
  SOP ships with bundled entry points for the web-based Data Manager and associated tooling. Language packages may provide wrapper CLI commands, and repository-level scripts orchestrate tests and example runs across multiple surfaces. This gives teams a practical operational interface alongside the library APIs.
  Sources: GETTING_STARTED.md, tools/httpserver/README.md, DEV_TOOLS.md

- Item: Platform tools extend the storage engine across languages
  Summary: The platform tools layer brings GUI, scripting, REST, and AI tooling to the proven storage core.
  Summary: The release model is intentionally multi-language, spanning Go, C#, Java, Python, and Rust.
  Summary: The tooling layer is positioned as a productivity surface on top of the same transactional engine.
  Body:
  SOP positions the platform tools as a productivity layer rather than a separate storage product. The same storage engine remains responsible for transactions, coordination, and durability, while the tools add Data Manager, REST APIs, scripting, and AI-facing workflows. This surface is intentionally distributed across the supported language ecosystems so teams can keep one storage foundation while using the tooling from their preferred runtime.
  Sources: SOP_PLATFORM_TOOLS.md, MULTI_LINGUAL_SUPPORT.md, tools/httpserver/README.md

# Developer Workflow

This category captures the day-to-day development, scripting, automation, and maintenance workflows used around SOP.

## Build & Run

This category covers the main ways developers build, launch, and interact with SOP components during local and packaged use.

- Item: Code-first and data-first workflow split
  Summary: SOP supports both code-first and data-first workflows.
  Summary: Code-first starts in application code and later uses the Data Manager for inspection.
  Summary: Data-first starts in the Data Manager and later opens the created stores from application code.
  Body:
  SOP supports two common ways of working. In the code-first path, developers define stores and logic in application code and use the Data Manager afterward for inspection and debugging. In the data-first path, teams create stores, schema, and seed data in the Data Manager first, then consume those stores from code. Both patterns operate on the same underlying storage model.
  Sources: README.md, WORKFLOWS.md

- Item: Build and run surface inventory
  Summary: SOP provides direct `go run` paths, bundled binaries, and shell scripts for build and test operations.
  Summary: The HTTP server and Data Manager can be run directly from source or from distribution bundles.
  Summary: Multi-step test and release flows are scripted at the repository root.
  Body:
  The repository exposes several entry points for building and running SOP components. The Data Manager can be launched from source or through bundled binaries, while root-level scripts coordinate broader test and build flows. This gives developers flexible options for local development, packaging, and validation.
  Sources: README.md, GETTING_STARTED.md, DEV_TOOLS.md

- Item: Data Manager UI behavior is metadata driven
  Summary: The UI is moving toward metadata-driven rendering rather than hardcoded store-specific display logic.
  Summary: Browser-side preferences also control mobile-mode behavior for the Data Manager experience.
  Summary: This keeps the management surface adaptable without baking every store rule into frontend code.
  Body:
  The Data Manager UI is documented as moving away from hardcoded per-store rendering rules toward a metadata-driven model where UI schema is stored and interpreted as data. The same UI architecture also persists presentational preferences such as mobile behavior in browser storage. Together, these choices make the interface easier to extend without continually adding one-off frontend conditionals.
  Sources: UI_ARCHITECTURE.md, tools/httpserver/README.md

## Scripts & Automation

This category describes how deterministic scripts, HTTP surfaces, and orchestration patterns are used to automate SOP workflows.

- Item: Explicit script authoring workflow
  Summary: SOP scripts should be built incrementally from small verified units.
  Summary: A narrow atomic script should be validated before being composed into a controller workflow.
  Summary: Verified scripts can then be promoted into reusable scheduled automation.
  Body:
  The script system works best when workflows are authored incrementally. Start with one atomic script that solves a narrow business rule, validate it with representative inputs, and then build a controller script that calls it as part of a larger loop or orchestration flow. This keeps scripts deterministic and reviewable and avoids asking the model to regenerate whole workflows repeatedly.
  Sources: ai/AI_SCRIPT_ARCHITECTURE.md, README.md

- Item: Automation through HTTP and managed tools
  Summary: SOP automation can be driven through scripts, HTTP surfaces, and the Data Manager.
  Summary: The same system supports local UI interaction and remote script execution.
  Summary: This allows one automation surface to serve both operator workflows and application integration.
  Body:
  SOP's automation surfaces are not limited to one interface. The same stored script concepts can be used through the management UI, through HTTP-based invocation, and through application integrations in other languages. This helps keep automation logic centralized while allowing different clients to trigger it in their preferred way.
  Sources: README.md, tools/httpserver/README.md, ai/cmd/knowledge_compiler/README.md

- Item: Scripts support swarm and saga orchestration
  Summary: SOP scripts can run parallel async steps and compose multi-database workflows through saga-style boundaries.
  Summary: Synchronous nested scripts can inherit transactions, while async steps are detached to avoid unsafe shared writes.
  Summary: Tool registry and script introspection make automation inspectable rather than opaque.
  Body:
  The agent scripting model is designed for deterministic workflow execution rather than opaque prompt-only automation. Scripts can compose synchronous subroutines that inherit transactions, asynchronous swarm steps that run detached from the parent transaction, and saga-style database-scoped workflows across multiple stores or databases. The structured tool registry keeps these automation paths inspectable and easier to validate.
  Sources: ai/agent/README.md, ai/AI_SCRIPT_ARCHITECTURE.md, README.md

## Examples

This category explains how the example set should be used as a practical onboarding and experimentation path.

- Item: Example-driven learning path
  Summary: The example set acts as a practical learning path from basic storage to more advanced interoperability and clustering patterns.
  Summary: Examples are intended to be copied, run, and adapted rather than treated only as static documentation.
  Summary: They complement the cookbooks by showing complete runnable flows.
  Body:
  Examples provide a hands-on route through the platform, starting with smaller CRUD and interop demonstrations and moving into more advanced transactional, vector, and clustered scenarios. They are intended to complement cookbook snippets by showing complete runnable programs.
  Sources: examples/README.md, bindings/python/sop/examples/README.md

## Contributing / Maintenance

This category summarizes the repository structure and maintenance surfaces that support ongoing SOP development.

- Item: Repository layout and maintenance surfaces
  Summary: The repository is organized by concern, with core storage, bindings, AI systems, and tools separated into dedicated directories.
  Summary: Root-level guides provide platform overviews, while subdirectory READMEs document feature-specific behavior.
  Summary: Tests are colocated with implementation and operational scripts live at the repository root.
  Body:
  The repository layout is designed to separate the storage engine, language bindings, AI systems, and operational tools into dedicated areas. Root-level guides cover platform-wide concerns, while subdirectories carry feature-specific documentation. Tests remain close to implementation, and root-level scripts support maintenance and verification tasks.
  Sources: README.md, ARCHITECTURE.md, DEV_TOOLS.md
