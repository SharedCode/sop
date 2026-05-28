# Implementation

This document tracks architecture intent, what is implemented, and explicit TODO items.

## Architecture Philosophy

- Avoid hardcoding prompt-engineering limits in the Go binary.
- Keep architectural constraints in playbooks and knowledge bases so behavior can evolve without backend redeploys.
- Drive alignment through deterministic context injection and explicit routing, not ad-hoc semantic guessing.

## Status Snapshot

### Implemented

- Three-gate intent routing is active.
- Focused context assembly runs after classification.
- Stores prompt context is artifact-scoped and relation-aware.
- CRUD-scoped operation guidance is injected for focused execution.
- Batch-first CRUD and search patterns are established in the AI layer.
- SearchByPath lexical fast-path is active.

### TODO

- [ ] Document-to-Space tool for promoting resolved insights into curated Spaces.
- [ ] Clarification fallback tool for unresolved ambiguity after autonomous research.
- [ ] Additional Omni consumption hardening for cross-database loopback flows.
- [ ] Medical Space showcase package for deep-domain demonstration.

### Document To Space (Declarative vs Episodic Bridge)

Concept carried from the previous design plan:

- Curated UI spaces are declarative knowledge assets.
- Conversational memory is episodic and auto-enriched.
- The platform needs an explicit bridge tool so high-value resolved knowledge can be promoted from episodic flow into curated space content.

TODO details:

- [ ] Add a document_to_space tool contract.
- [ ] Define when assistant should propose promotion.
- [ ] Define moderation and review gate for promoted content.

## 1. Context Assembly Protocol

To avoid brittle RAG retrieval for critical tool definitions, context assembly follows deterministic routing and then deterministic expansion.

### Problem Statement

If critical tool manuals are fragmented and left to probabilistic retrieval, a degraded reasoning loop can miss required constraints and emit malformed scripts.

### Three-Gate Routing Architecture

Gate 1: Focused prefix routing.

- Input shape: explicit namespace such as omni:stores:users.
- Action: parse hard constraints and classify only the missing parts (mainly layers and CRUD intent).
- Result: deterministic route with low token overhead.

Gate 2: MRU continuity or switch routing.

- Input shape: query without explicit prefix, with inherited routing state.
- Action: verify continuation vs topic switch and update CRUD/layer scope.
- Result: preserve momentum while preventing topic drift.

Gate 3: cold-start discovery routing.

- Input shape: no prefix and no valid inherited context.
- Action: classify from a lightweight context outline (entities/domains/artifacts).
- Result: discover entity, domain, db artifacts, and layers from scratch.

### Focused Context Assembly (Post-Classification)

Classification output is intentionally compact. It must be expanded before prompt construction.

Design gap that was identified and fixed:

- The previous pipeline classified correctly but injected broad domain context.
- The missing step was deterministic expansion between classification and final prompt assembly.
- The gap was not in the classifier contract; it was in orchestration between routing and prompt construction.

Resolved design:

- Keep the three gates as classifiers only.
- Expand classification via a deterministic context assembler.
- Inject the expanded payload as a dedicated prompt component for focused execution context.

Insertion-point guidance:

- Conceptually correct location: immediately after routing succeeds and before final prompt build.
- Low-churn implementation path: expansion can be invoked during prompt construction as long as classification remains pure and expansion remains deterministic.

- Input contract: Domain, DBArtifacts, Layers with CRUD tags.
- Domain scope: inject only Stores or Spaces operating context.
- Artifact scope: inject only the classified targets.
- CRUD scope: inject only relevant API and operation guidance.
- Relation scope: inject store relation metadata needed for joins/traversal.

Artifact expansion checklist:

- store name
- description when present
- inferred field/schema
- key schema when present
- relations
- optional tiny sample shape only when useful

Example envelope for Stores + users + R:

- users schema details
- users relations
- read transaction and AST guidance such as open_db, begin_tx read, open_store, scan, filter, project, join, limit

CRUD-to-API expansion mapping:

- C maps to create/save/upsert flows.
- R maps to read flows such as open_store, scan, get/find/select/filter patterns.
- U maps to mutation flows such as update/patch/save.
- D maps to delete/remove flows.

Suggested internal model:

- IntentExecutionContext assembled from TaskContextClassification.
- Prompt assembly consumes only IntentExecutionContext, not raw coarse classification.

### Hybrid Tool Injection Strategy

- Tool context must be constrained by CRUD tags.
- If D is not tagged, delete-oriented guidance should not be injected.
- If only R is tagged, read-first execution guidance should dominate the context.

## 2. Domain Guardrails and Clarification

### Objective

Prevent unknown-unknown hallucinations when schema links or constraints are missing.

### Guardrail Model

- Use semantic overrides in KB instructions rather than binary hardcoding.
- Require schema-mapping validation before generating AST scripts.
- Enforce halt-and-clarify behavior when ambiguity remains after research.

### Clarification Workflow

1. Autonomous research first using read/search tools.
2. If unresolved, explicitly ask the user for missing constraints.

### TODO

- [ ] Formalize a dedicated ask_user or clarify_intent tool contract in this implementation track.

## 3. Roadmap and Release Details

### V1 Release Scope

### Foundational Goal

Production-ready knowledge base and orchestration core with deterministic context assembly and scalable data access.

### Target Deliverables

1. Batched-first native CRUD API parity across SDK and UI flows.
2. Omni knowledge consumption via transactional loopback and retrieval orchestration.
3. Lexical fast-path via SearchByPath.
4. Medical knowledge base demonstration as a deep-domain benchmark.

### Current Completion Markers

- Batched-first CRUD behavior: implemented.
- SearchByPath fast prefix scanning: implemented.
- Three-gate routing and focused context assembly: implemented.
- Medical demo packaging: TODO.

## 4. V1.5 and V2 Deferred Scope

- Stateful avatar systems with richer specialization.
- Dynamic multi-avatar switching during live interaction.
- Deeper long-term episodic memory and persona continuity.

## Maintenance Guidance

- Keep implemented items and TODOs explicit.
- Prefer status-oriented updates over free-form narrative drift.
- Preserve architecture rationale when refining implementation details.