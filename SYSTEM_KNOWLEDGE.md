# SOP System Knowledge Base & Handoff

**Last Updated:** January 12, 2026
**Purpose:** This document serves as a knowledge transfer artifact for future AI agents (e.g., GPT-5.2) or developers continuing work on the SOP Data Manager. It details the system architecture, specific implementation nuances, and recent modifications.

---

## 1. System Overview

**SOP (Scalable Objects Persistence)** is a high-performance, NoSQL database engine built in Go. It uses B-Trees for storage and supports ACID transactions.

*   **Core**: Go-based B-Tree engine (`github.com/sharedcode/sop`).
*   **UI**: A web-based "Data Manager" tool (`tools/httpserver`), serving HTML templates and a WebSocket/Rest API.
*   **AI Agent**: An intelligent agent (`ai/agent`) integrated into the Data Manager to convert Natural Language into database operations.

---

## 2. AI Agent Architecture

The AI Agent is a "ReAct" (Reason + Act) style agent defined in `ai/agent/`.

### Tool Registry (`ai/agent/dataadmintools.go`)
The agent exposes tools to the LLM via a registry.
*   **`select`**: High-level tool to query data.
*   **`join`**: High-level tool to join two stores.
*   **`execute_script`**: A low-level, powerful tool that executes a pipeline of atomic operations ("Lego Blocks").

### Scripting Engine (`ai/agent/dataadmintools.atomic.go`)
The `execute_script` tool accepts a JSON array of operations. This is the "Assembly Language" of the agent.
*   **Key Operations**:
    *   `open_db`, `begin_tx`, `open_store`
    *   `scan`: Iterates a B-Tree (returns full objects).
    *   `join_right`: Performs a stream-based right outer join (returns combined objects).
    *   `project`: Filters and renames fields (CRITICAL for column selection).
    *   `limit`: Restricts the number of results.

---

## 3. Projection & Field Selection Logic (Critical Nuances)

We have implemented sophisticated logic to handle SQL-style projections (`SELECT a.*, b.name AS employee`) within the NoSQL pipeline.

### The Problem
SOP stores are B-Trees storing complete objects. Joins merge these into composite maps like `{"department": "Sales", "Right.name": "Alice"}`. Users want precise control over the output JSON keys.

### The Solution: `renderItem` & `parseProjectionFields`
Located in `ai/agent/dataadmintools.utils.go`.

#### Supported Field Formats
1.  **Wildcard**: `["*"]` or `nil` or `[""]` (empty string in list).
    *   Returns the flattened object.
    *   **Fix**: We explicitly handle `[""]` (which some LLMs generate erroneously) as a wildcard.
2.  **List of Strings**: `["field1", "field2"]`
    *   Preserves order.
3.  **Aliases**: `["field AS alias", "field2 as alias2"]`
    *   Case-insensitive " AS " parsing.
4.  **Scoped Wildcards**: `["a.*"]`, `["left.*"]`, `["b.*"]`
    *   **Logic**:
        *   `a.*` / `left.*`: Returns all fields *excluding* those with `Right.` prefix.
        *   `b.*` / `right.*`: Returns *only* fields with `Right.` prefix (and keeps/strips prefix based on need, currently keeps collision avoidance).
    *   **Mix & Match**: You can do `["a.*", "b.name AS Employee"]`.
        *   The system expands `a.*` first (appending keys).
        *   Then adds `Employee` (aliased from `Right.name`).

#### Key Implementation Details
*   **`parseProjectionFields`**: Parses raw input into a list of `ProjectionField{Src, Dst}`.
*   **`renderItem`**: Takes `key`, `val` (or a joined map) and the parsed fields. It constructs an `OrderedMap` to ensure the JSON output matches the requested field order.
*   **Wildcard Expansion**: Inside `renderItem`, if a field source ends in `*` (e.g., `a.*`), it calls `flattenItem` to get all available keys and filters them based on the prefix (Scope).

### Internal Data Representation: **"Always Prefixed"**
To resolve ambiguities in Joins and Projections, the system now enforces a strict **Namespaced Internal Representation**.
*   **Storage**: B-Trees store raw objects (un-prefixed field names like `name`, `id`).
*   **Pipeline (`scan`)**: The `scan` operation serves as the **Presentation Adapter**. It automatically prefixes all fields with the store name (e.g., `{"name": "Alice"}` becomes `{"users.name": "Alice"}`).
*   **Pipeline Flow**: All downstream operations (`join`, `project`, `filter` *after scan*) see fully qualified keys.
*   **Output (`renderItem`)**: The projection layer is responsible for stripping these prefixes for the final user output, unless aliases are used to preserve them.
*   **Filtering**: Note that `scan` filtering usually happens *before* this prefixing (using the store's local schema), but downstream filters must use the prefixed names.

---

## 4. Documentation & LLM Instructions

### `ai/agent/dataadmintools.go`
The tool descriptions here are the **source of truth** for the LLM.
*   **`execute_script`**:
    *   Explicitly warns: `'scan' and 'join' return full objects. To select specific fields or renamed columns, you MUST add a 'project' step.`
    *   Example pipeline updated to include a `project` step.
*   **`select` / `join`**:
    *   `fields` argument spec: `list<string>, optional. Use ['*'] or nil for all fields. Supported formats: ['*'], ['field1', 'field2'], ['field AS alias']`.

### `ai/AI_COPILOT_USAGE.md`
User-facing documentation. We generally do *not* put internal prompt instructions here, keeping it clean for the end-user.

---

## 5. UI Customization

### Chat Interface (`tools/httpserver/templates/index.html`)
*   **Input Box**: Default height increased to `60px` (min `40px`) to accommodate multi-line queries better.
*   **CSS**: `#chat-input` styling rules.
*   **Step Visibility**: The prompt for the step number is now rendered as "Step [N]:" (e.g., "Step 1:").
    *   Logic ensures `step_index` is checked strictly (`!== undefined && !== null`).
    *   Fallback for missing index displays "Step (No Index):".

### Backend UI Rendering (`tools/httpserver/main.ai.go`)
*   **Event Interception**: The chat backend intercepts NDJSON events from the execution engine.
*   **Structured Formatting**: Parses the new structured `step_start` events to render distinct step headers (e.g., `**Step <N>:**`).
    *   Extracts `step_index`.
    *   Uses `command` as fallback if `name` is missing.

### Script Execution Architecture (`ai/agent/service.runner.go`)
*   **Structured Execution Results**: The engine now emits discrete, structured events for each execution step (`step_start`, `record`, `outputs`) rather than a single blob. This enables granular real-time feedback.
*   **Context Propagation**: `step_index` and metadata are propagated via `context.Context` to ensure deep nested calls (streaming or legacy fallback) can correctly attribute their output to the specific step.

---

## 6. Development & Testing

### Key Test Files
*   **`ai/agent/dataadmintools_utils.go`**: Logic for projection parsing.
*   **`ai/agent/limit_ordering_test.go`**: Tests for `project`, `limit`, and order preservation.
*   **`ai/agent/dataadmintools_join_repro_test.go`**: Regression tests for joins and alias collisions.

---

## 7. Join Optimization & JIT Planning

We implemented a **Just-In-Time (JIT) Planner** within the `JoinProcessor` (`ai/agent/dataadmintools.join.go`) to optimize Joins dynamically based on the B-Tree structure.

### Hash Join vs. Lookup Join
The Join Processor chooses between:
1.  **Lookup Join (Preferred)**: Uses B-Tree `Find()` and `Next()`. Complexity: $O(M \times \log N)$.
2.  **Hash Join (Fallback)**: Scans Right store completely into memory. Complexity: $O(M + N)$.

### Optimization Logic
1.  **Prefix Validation** (`validateIndexPrefix`):
    *   Verifies that the `JOIN ON` columns match the **Right Store's Index Prefix**.
    *   Checks strictly (must start at Index Field 0) but allows partial prefixes (e.g., Index `[A, B]`, Join `[A]`).
    *   **Mapped Names**: Resolves user aliases (e.g., `join on b`) to internal Store Field Names (e.g., `B`) before checking.
2.  **Sort Optimization** (`checkRightSortOptimization`):
    *   If the query requests `ORDER BY a.field, b.field DESC`:
        *   Standard behavior: Buffer Right matches in memory and sort using Go `sort.Slice`.
        *   **Optimization**: If the **Right B-Tree Index** naturally provides this order (e.g., Index is `[A ASC, B DESC]`), we **disable buffering**.
    *   This logic strictly compares Field Names and Sort Direction (Asc/Desc) against the B-Tree spec.
3.  **Execution**:
    *   The `Execute` method runs these checks *before* processing any rows.
    *   If `isRightSortOptimized` is true, `processLeftItem` streams results directly, achieving maximum performance.

### Efficient Query Scenarios (Supported)

The following scenarios leverage the B-Tree structure for **Fast Lookup** ($O(M \log N)$) instead of Full Table Scan ($O(N)$):

| Scenario | Index Spec | Join Condition | Order By | Status | Reason |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Exact Full Key** | `[A, B]` | `ON b.A=.. AND b.B=..` | (Any) | **Fast** | Full match. |
| **Prefix Match** | `[A, B]` | `ON b.A=..` | (Any) | **Fast** | Partial prefix A is valid for lookup. |
| **Sort Optimization (Value)** | `[A, B]` | `ON b.A=..` | `ORDER BY b.B` | **Fast + Stream** | `B` follows `A` in index. Sorted natively. |
| **Sort Optimization (Direction)** | `[A ASC, B DESC]` | `ON b.A=..` | `ORDER BY b.B DESC` | **Fast + Stream** | Requested `DESC` matches Index `DESC`. |

### Inefficient Scenarios (Fallback to Hash Join)

| Scenario | Index Spec | Join Condition | Order By | Status | Reason |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Skip First Field** | `[A, B]` | `ON b.B=..` | (Any) | **Slow (Scan)** | Cannot seek B-Tree without prefix `A`. |
| **Sort Mismatch** | `[A, B]` | `ON b.A=..` | `ORDER BY b.B DESC` | **Buffered Sort** | Index is `B ASC`, Query wants `B DESC`. Requires memory sort. |
| **Sort Gap** | `[A, B, C]` | `ON b.A=..` | `ORDER BY b.C` | **Buffered Sort** | Skipping `B` breaks sort order continuity. |

*   **`ai/agent/dataadmintools_select_ordered_test.go`**: Tests for simple select ordering.

### Running Tests
```bash
# Run all agent tests
go test -v ./ai/agent

# Run specific test
go test -v ./ai/agent -run TestProjectLimitOrdering
```

## 7. Known Quirks / Future Work
*   **LLM "Empty String" Wildcard**: Some models return `[""]` for "all fields". We patched this in `dataadmintools.utils.go`, but better prompt engineering in `dataadmintools.go` is the long-term fix (already attempted).
*   **"Right." Prefix**: The Join operation (Right Join) currently prefixes fields from the "Right" store with `Right.`. The `project` step is often needed to rename these to cleaner names (e.g., `Right.name` -> `employee_name`).

---

## 8. Memory Architecture

The system implements a distinct separation between transient context and persistent knowledge.

### Short-Term Memory (Session Context)
*   **Implementation**: `RunnerSession` struct in `ai/agent/memory_shortterm.go` (and `service.session.go`).
*   **Scope**: Transient. Exists only for the duration of the current user session (WebSocket connection).
*   **Components**:
    *   **History**: A transcript of the conversation (User/Assistant messages).
    *   **Variables**: A generic map[`string`]any for storing session-scoped objects (e.g., active specific DB transactions `tx1`, result streams `stream_cursor`).
    *   **Transaction**: The current active global transaction (if any).
    *   **CurrentScript**: Buffer for script lines being drafted before execution.

### Long-Term Memory (System Knowledge)
*   **Implementation**: `llm_knowledge` B-Tree store in the `system` database. Managed via `KnowledgeStore` in `ai/agent/memory_longterm.go`.
*   **Scope**: Persistent. Survives server restarts and spans across sessions/users.
*   **Structure**: Uses a composite key (`Category`, `Name`).
*   **Mechanism**:
    *   **Dynamic Instruction Loading**: The `DataAdminAgent` uses `getToolInstruction` (`ai/agent/dataadmintools.go`) to look up tool descriptions in the knowledge store (Category: "tool") before registering them. This allows the agent's behavior (prompts) to be patched or improved without recompiling the code.
    *   **Self-Correction**: The `manage_knowledge` tool exposes this B-Tree to the agent, allowing it to save new terms, definitions, or instructions (Self-learning loop).

---

## 9. System Database & Deployment

The **System Database** is a dedicated SOP database used to store internal metadata, configuration, and the Agent's own operating manual.

### "Self-Correcting" Storage (`llm_knowledge`)
The Agent does not rely solely on hardcoded prompts. It fetches tool usage instructions from a B-Tree store named `llm_knowledge` located within the System DB.
*   **Structure:** Composite Key B-Tree (`Category`, `Name` -> `Content`).
*   **Key:** Tool Name (e.g., `"execute_script"`).
*   **Value:** Complete instruction text/prompt.
*   **Seeding:** On server startup (`tools/httpserver/main.ai.go`), the system checks if instructions exist. If not, it seeds them with defaults hardcoded in the binary.
*   **Self-Correction:** The Agent has a tool `update_instruction` that allows it to rewrite these entries, effectively updating its own "brain" persistently.

### Setup Wizard Configuration
The Setup Wizard (`tools/httpserver/templates/index.html`) supports three configuration modes for the System DB:
1.  **Enterprise Brain (Managed):**
    *   Activated when a System DB is detected in the active configuration (whether from config file, environment, or manual selection).
    *   UI displays a "Chip/Brain" icon to indicate that the instance is connected to a shared system intelligence.
    *   Ensures all instances share the same "corporate brain".
2.  **Custom Existing DB:**
    *   User manually enters a path to an existing System DB.
3.  **Local Environment:**
    *   Default behavior. Creates a new System DB alongside the User DB (e.g., `.../sop_data/system_db`).

**Configuration Default:**
*   **Mode:** Defaults to **Clustered** (Redis-backed) to support embedded/k8s scaling scenarios, though "Standalone" is available for simpler local tests.

---

## 10. Scaling Strategy (Vector Store)

### Current State: B-Tree Lookup
Currently, the Agent retrieves instructions using **Exact Match** lookup from the `llm_knowledge` B-Tree. This is efficient for the current set of tools (~20-50).

### Vector Store Integration (`ai/vector`)
A sophisticated **Transactional Vector Store** (IVF-based) is implemented in the codebase but is **not currently auto-scaling**.
*   **Status:** "Opt-in" / "Up in the air".
*   **Usage:** Must be explicitly enabled in the Agent configuration (`vector_store: true`).
*   **Gap:** There is currently **no auto-migration logic** that transitions from the KV store to the Vector Store when the instruction set grows too large. This "auto-scale to RAG" feature is planned but not implemented.

---

## 11. AI Feedback Loop (Implemented Jan 2026)

A fully functional feedback mechanism ("Thumb Up/Thumb Down") has been integrated into the `scripts.html` UI and the backend (Jan 24-25, 2026). This system enables the storage of user feedback on AI responses to improve future performance (RLHF/RAG).

### 11.1 Architecture

*   **Frontend (`tools/httpserver/templates/scripts.html`)**:
    *   Intercepts clicks on feedback buttons (Thumbs Up/Down).
    *   Constructs a payload containing the `UserContent` (Query) and `AIResponse`.
    *   Sends a `POST` request to `/api/ai/feedback`.
    *   Visual feedback (turning icons green/red) is handled locally.

*   **Backend (`tools/httpserver/main.ai.go`)**:
    *   **Endpoint**: `handleAIFeedback`.
    *   **Storage Strategy**: Hybrid (B-Tree + Vector Store).
        *   **Raw Data**: The full JSON object corresponds to `llm_feedback` B-Tree in the `system` database. This is the System of Record.
        *   **Semantic Index**: Incoming queries are embedded using `embed.NewSimple` (Hashing Embedder, 384 dim) and stored in the `llm_feedback` Vector Store.
    *   **Transactionality**: Both operations share a single ACID transaction (`database.BeginTransaction`).

### 11.2 Current Limitations & Roadmap

1.  **Embeddings**: Currently uses a hashing embedder (deterministic but not semantic).
    *   *Next Step*: Replace with a semantic model (e.g., OpenAI/BERT) in `ai/embed`.
2.  **Retrieval (RAG)**: Data is stored but not queried.
    *   *Next Step*: Update `sql_admin` agent to query `llm_feedback` vectors for similar past queries before generating answers.
3.  **Policy Enforcement**:
    *   *Next Step*: Convert negative feedback clusters into "Do Not" policies injected into the system prompt.

---

## 12. Coding Standards & Guidelines

To ensure maintainability and professional code quality, adhere to the following strict guidelines when refactoring or adding new code.

### File Size Limits
*   **Maximum Lines per File:** **750 lines**.
*   **Action:** If a file exceeds or approaches this limit, refactor immediately by extracting logical components into separate files (e.g., helpers, handlers, types) or sub-packages.

### Commenting Style
*   **Objective**: Comments must be purely technical and descriptive.
*   **Focus**: Explain **what** the code does, **why** a specific complex logic is used, and the **flow** of execution.
*   **Prohibited**:
    *   Do **not** write apologetic or explanatory narratives about mistakes (e.g., "We made a mistake here previously...").
    *   Do **not** include "obsolete drama" or conversational meta-commentary.
    *   Do **not** state "This is a workaround" without a technical reason.
*   **Example (Good)**: `// Collect all target paths to validate isolation and permissions.`
*   **Example (Bad)**: `// Sorry, we used to deduplicate here but that caused a bug so now we blindly append...`

### Modularity
*   **Function Size**: Functions must **not exceed 500 lines**.
*   **Strategy**: Decompose complex logic into named sub-functions with clear, single responsibilities. Use helper functions to keep the main flow readable.

---

## 8. Type System & Inference Strategy (Updated Jan 2026)

### Type Erasure & Flexible Binding
*   **No Explicit Persistence**: We deliberately **removed** `KeyTypeName` and `ValueTypeName` from `StoreInfo`. Storing Go type names proved brittle and unnecessary given the multi-language nature of SOP.
*   **Late Binding**: The system relies on **runtime type inference** rather than persisted metadata.
*   **Sampling Strategy**: The "Data Manager" UI determines the display type for a Store by **sampling the first item** in the B-Tree.
    *   This logic is centralized in `sop.InferType` (`types.go`).
    *   It handles `UUID` (sop/native/string), `time.Time`, and primitives.

### Supported Primitives
*   The system intentionally supports a rich set of primitives (int, float, string, UUID, time) matching `btree/comparer.go`.
*   **Exclusion**: Single `byte` or `char` types are **not** supported as B-Tree keys, as they are deemed unnecessary for persistence use cases. Use `string` or `[]byte` (Blob) instead.
