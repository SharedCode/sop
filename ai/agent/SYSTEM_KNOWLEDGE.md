# SOP System Knowledge Base & Handoff

**Last Updated:** January 9, 2026
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

---

## 4. Documentation & LLM Instructions

### `ai/agent/dataadmintools.go`
The tool descriptions here are the **source of truth** for the LLM.
*   **`execute_script`**:
    *   Explicitly warns: `'scan' and 'join' return full objects. To select specific fields or renamed columns, you MUST add a 'project' step.`
    *   Example pipeline updated to include a `project` step.
*   **`select` / `join`**:
    *   `fields` argument spec: `list<string>, optional. Use ['*'] or nil for all fields. Supported formats: ['*'], ['field1', 'field2'], ['field AS alias']`.

### `ai/AI_ASSISTANT_USAGE.md`
User-facing documentation. We generally do *not* put internal prompt instructions here, keeping it clean for the end-user.

---

## 5. UI Customization

### Chat Interface (`tools/httpserver/templates/index.html`)
*   **Input Box**: Default height increased to `60px` (min `40px`) to accommodate multi-line queries better.
*   **CSS**: `#chat-input` styling rules.

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
**End of Handover Document**
