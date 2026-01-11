# SOP Agent Framework

The SOP Agent Framework (`ai/agent`) is a powerful system for building autonomous agents that can execute complex workflows, interact with databases, and orchestrate multi-step tasks.

## Core Concepts

### 1. Scripts ("Natural Language Programming")
Scripts (formerly Scripts) are the building blocks of agent behavior. They are JSON-based functional scripts that define a sequence of steps.
*   **Compiled Instructions**: Scripts are parsed and executed by the `Service`.
*   **Inspectable**: Tools and steps are defined in a registry, allowing for validation and introspection.

### 2. Swarm Computing (Async Execution)
The framework supports "Swarm Computing" by allowing steps to run in parallel.
*   **Async Steps**: Any step can be marked as `"is_async": true`.
*   **Transaction Isolation**: Async steps are **detached** from the parent transaction. They must start their own transaction if they need to write to the database. This prevents race conditions and allows for "In-Flight Data Merging" at the storage layer.
*   **Nested Swarms**: A step of type `call_script` can be run asynchronously even if the parent script is in a transaction. The nested script will start with a detached context and can establish its own transaction (e.g., on a different database), enabling powerful hierarchical agent swarms.
*   **Safeguards**: For non-script steps (like `command` or `set`), the system enforces synchronous execution if an active transaction exists to prevent accidental data corruption.
*   **Error Propagation**: By default, if an async step fails, it cancels the entire group. You can override this with `"continue_on_error": true`.

### 3. Transaction Inheritance (Subroutines)
If a nested script is run **synchronously** (`"is_async": false`) and does **not** specify a different database, it acts as a **Subroutine**.
*   **Inheritance**: It inherits the parent's active transaction.
*   **Atomicity**: Operations in the subroutine are part of the parent's atomic unit of work. If the subroutine fails, the parent transaction rolls back.

### 4. Saga Pattern (Multi-Database Workflows)
SOP encourages the **Saga Pattern** for workflows that span multiple databases. Instead of a single distributed transaction (which is complex and brittle), you chain scripts where each script operates on a specific database.

*   **Database-Scoped Scripts**: A script can specify a `"database"` field.
*   **Automatic Transaction Wrapping**: When a script specifies a database, the system automatically:
    1.  Switches the context to that database.
    2.  Starts a new transaction (`ForWriting`).
    3.  Commits the transaction if the script succeeds.
    4.  Rolls back if it fails.

### 4. Tool Registry
Tools are no longer hardcoded strings. They are defined in a structured `Registry`.
*   **Definition**: Tools have a Name, Description, Argument Schema, and a Go Handler function.
*   **Discovery**: Agents can dynamically list and inspect available tools to generate their system prompts.

### 5. Explicit Transaction Management
While automatic transaction wrapping (Saga Pattern) is recommended for most cases, you can still manage transactions explicitly using the `manage_transaction` tool. This is useful when:
*   **Minimizing Lock Time**: You want to perform heavy AI processing or external API calls *outside* of a transaction, and only wrap the database writes.
*   **Multiple Transactions**: A single script needs to perform multiple independent commits.
*   **Inherited Context**: You are running a script without a specific `database` field and want to control the transaction on the inherited database connection.

---

## Script Schema

A `Script` consists of metadata and a list of `Steps`.

```json
{
  "name": "example_script",
  "description": "Demonstrates async and db features",
  "parameters": ["user_id"],
  "database": "users_db", // Optional: Target DB for this script
  "steps": [ ... ]
}
```

### Common Step Fields
All steps support the following optional fields to improve readability and control:
*   `name`: A unique identifier for the step (useful for logs and future jumps).
*   `description`: A human-readable explanation of the step's intent.

### Step Types

| Type | Description | Key Fields |
| :--- | :--- | :--- |
| `ask` | Ask the LLM a question | `prompt`, `output_variable` |
| `set` | Set a variable | `variable`, `value` |
| `if` | Conditional logic | `condition`, `then`, `else` |
| `loop` | Iterate over a list | `list`, `iterator`, `steps` |
| `fetch` | Retrieve data from B-Tree | `source`, `resource`, `variable` |
| `command` | Execute a registered tool | `command`, `args` |
| `script` | Run a nested script | `script_name`, `script_args` |

### Async & Error Handling Fields

*   `"is_async": true`: Runs the step in a background goroutine.
*   `"continue_on_error": true`: If this step fails, do not stop the rest of the script.

## System Tools for Script Authoring
The Agent can manage its own scripts using the following registered tools:
*   **`create_script`**: Initialize a new empty script with a description.
*   **`save_script`**: Save (overwrite) a complete script structure.
*   **`save_step`**: Append a single step to the end of a script. This supports a "stream of thought" authoring process where the agent builds a program incrementally.
*   **`insert_step` / `update_step`**: Edit existing logic in place.

---

## Examples

### 1. Async Execution (Swarm)

Run two heavy tasks in parallel.

```json
{
  "name": "parallel_processing",
  "steps": [
    {
      "type": "command",
      "command": "heavy_computation",
      "args": {"id": "1"},
      "is_async": true
    },
    {
      "type": "command",
      "command": "heavy_computation",
      "args": {"id": "2"},
      "is_async": true
    }
  ]
}
```

### 2. Saga Pattern (Multi-DB)

Orchestrate a workflow across two databases using nested scripts.

**Script 1: Update User (on `users_db`)**
```json
{
  "name": "update_user",
  "database": "users_db",
  "parameters": ["uid"],
  "steps": [
    { "type": "command", "command": "update_record", "args": {"id": "{{.uid}}"} }
  ]
}
```

**Script 2: Log Audit (on `audit_db`)**
```json
{
  "name": "log_audit",
  "database": "audit_db",
  "parameters": ["action"],
  "steps": [
    { "type": "command", "command": "insert_log", "args": {"msg": "{{.action}}"} }
  ]
}
```

**Script 3: Orchestrator (Saga)**
```json
{
  "name": "user_update_saga",
  "steps": [
    {
      "type": "script",
      "script_name": "update_user",
      "script_args": {"uid": "123"}
    },
    {
      "type": "script",
      "script_name": "log_audit",
      "script_args": {"action": "User 123 updated"}
    }
  ]
}
```

In this example:
1.  `update_user` runs in a transaction on `users_db`.
2.  If it succeeds, `log_audit` runs in a transaction on `audit_db`.
3.  If `update_user` fails, `log_audit` never runs.
4.  If `log_audit` fails, `update_user` is already committed (Saga pattern). You could add a compensation step to undo it if needed.

### 3. Batch Processing (Explicit Transactions)

Process a large list of items in batches, committing every few steps to avoid long-held locks or massive transactions.

```json
{
  "name": "batch_processing",
  "steps": [
    // Batch 1
    { "type": "command", "command": "process_item", "args": {"id": "1"} },
    { "type": "command", "command": "process_item", "args": {"id": "2"} },
    { "type": "command", "command": "manage_transaction", "args": {"action": "commit"} },
    
    // Batch 2 (Transaction automatically renewed after commit)
    { "type": "command", "command": "process_item", "args": {"id": "3"} },
    { "type": "command", "command": "process_item", "args": {"id": "4"} },
    { "type": "command", "command": "manage_transaction", "args": {"action": "commit"} }
  ]
}
```

This pattern ensures that if Batch 2 fails, Batch 1 remains committed.

---

## Data Engine & MOAT Features

The SOP Agent Framework includes a specialized **Data Admin Agent** that provides "bare metal" data manipulation capabilities. This implementation represents a significant competitive advantage (MOAT) due to its unique architecture.

### 1. Direct B-Tree Navigation (No Abstraction Layers)
Unlike traditional SQL engines that rely on thick layers of abstraction (SQL parsers, query optimizers, execution plans, storage engine APIs), SOP's tools interact **directly with the B-Tree cursors**.
*   **Efficiency**: Eliminates the overhead of query planning and intermediate object allocation.
*   **Control**: The agent has fine-grained control over navigation (`Next`, `Previous`, `FindOne`), allowing for optimizations that generic SQL optimizers often miss.

### 2. Zero-Copy Result Streaming (Cursor-to-Socket)
The `select` and `join` tools implement a rare and highly efficient **Zero-Copy Streaming** architecture.
*   **No Buffering**: Unlike standard ORMs or database drivers that load results into a massive slice or memory buffer before returning, SOP streams data **directly from the B-Tree cursor to the output stream**.
*   **Cursor-Driven**: As the B-Tree iterator advances (`Next()`), the current item is immediately serialized and flushed to the response (e.g., HTTP socket or console).
*   **Constant Memory**: This allows the agent to process, join, or select **millions of records** with a constant, minimal memory footprint (O(1) space complexity), regardless of the result set size.

### 3. 3-Way Merge Join Optimization
The `join` tool implements a sophisticated **Merge Join** strategy when joining on Primary Keys.
*   **Synchronized Scanning**: Iterates through both the Left and Right B-Trees simultaneously.
*   **Smart Seeking**: If one cursor falls behind, it doesn't just "scan" to catch up. It uses the B-Tree's `FindOne` capability to **jump** directly to the target key, skipping vast ranges of irrelevant data.
*   **Full Join Support**: Efficiently handles **Inner**, **Left**, **Right**, and **Full Outer** joins in a single pass, a capability rarely found in embedded or lightweight engines.

### 4. Smart Index Utilization
The `select` tool bridges the gap between "Point Lookups" and "Table Scans".
*   **Operator Awareness**: It analyzes filter criteria (e.g., `{"age": {"$gt": 25}}`).
*   **Index Seeking**: Instead of starting at the beginning of the table, it uses the B-Tree index to **seek** directly to the first matching record (e.g., the first user with age > 25) before beginning the scan.

### 5. Actionable Queries (Bulk Operations)
The engine supports performing actions directly within the query execution pipeline, enabling **high-performance, bare-metal bulk operations** without the need for "Select-then-Update" round trips.

*   **Bulk Delete**: The `select` tool accepts `action="delete"`. It iterates through the result set and removes items in-place.
    *   *Optimality*: Uses direct B-Tree cursor manipulation. It employs a "Peek-Next-Delete" strategy to ensure the cursor remains valid, achieving **O(N)** performance where N is the number of items to delete, with zero memory overhead for buffering.
*   **Bulk Update**: The `select` tool accepts `action="update"` and an `update_values` map. It merges the new values into the existing record and updates it in a single pass.
    *   *Efficiency*: Updates happen in-place. If the update doesn't change the key, it avoids expensive re-balancing.
*   **Join-Based Actions**: The `join` tool supports `action="delete_left"` and `action="update_left"`.
    *   *Complex Logic*: You can delete or update records in the Left table based on complex join conditions with the Right table (e.g., "Delete all Users who have no Orders" via a Left Join).
    *   *Performance*: These actions benefit from the same **Merge Join** and **Index Seeking** optimizations as standard queries. This allows for massive bulk updates across related tables with **optimal** I/O patterns.

### 6. Transaction Safety & Auto-Rollback
To ensure data integrity even when using powerful bulk operations or complex multi-step scripts:

*   **Auto-Rollback**: The Agent Runner enforces a "Clean Slate" policy. If a top-level script finishes execution (successfully or with an error) and leaves a transaction open (e.g., a user forgot to call `commit`), the runner **automatically rolls back** the transaction.
*   **Warning**: A warning is logged to the output, alerting the user that their uncommitted changes were discarded.
*   **Benefit**: This prevents "dangling transactions" from locking resources or leaking uncommitted state into subsequent agent interactions.

---

## Testing & Development

### Pipeline Integration Test Harness
We have a dedicated integration test harness for verifying the full RAG pipeline (Service + DataAdminAgent) end-to-end without running the full HTTP server. This is useful for debugging agent logic, tool execution, and session history recording.

**File**: `ai/agent/pipeline_integration_test.go`

**Features**:
*   **Real LLM Integration**: Uses the configured Gemini model to generate real plans.
*   **Stub Mode**: Runs with `StubMode: true`, so it simulates DB operations without side effects.
*   **Full Pipeline**: Tests the `Service` -> `Pipeline` -> `DataAdminAgent` flow.
*   **History Verification**: Verifies that `last-tool` correctly captures complex tool arguments (like scripts).

**How to Run**:
```bash
# Ensure GEMINI_API_KEY is set
export GEMINI_API_KEY="your_key_here"

# Run the specific test
go test -v ./ai/agent -run TestServiceIntegration_LastTool
```
