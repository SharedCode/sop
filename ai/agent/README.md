# SOP Agent Framework

The SOP Agent Framework (`ai/agent`) is a powerful system for building autonomous agents that can execute complex workflows, interact with databases, and orchestrate multi-step tasks.

## Core Concepts

### 1. Macros ("Natural Language Programming")
Macros are the building blocks of agent behavior. They are JSON-based scripts that define a sequence of steps.
*   **Compiled Instructions**: Macros are parsed and executed by the `Service`.
*   **Inspectable**: Tools and steps are defined in a registry, allowing for validation and introspection.

### 2. Swarm Computing (Async Execution)
The framework supports "Swarm Computing" by allowing steps to run in parallel.
*   **Async Steps**: Any step can be marked as `"is_async": true`.
*   **Transaction Isolation**: Async steps are **detached** from the parent transaction. They must start their own transaction if they need to write to the database. This prevents race conditions and allows for "In-Flight Data Merging" at the storage layer.
*   **Nested Swarms**: A step of type `macro` can be run asynchronously even if the parent macro is in a transaction. The nested macro will start with a detached context and can establish its own transaction (e.g., on a different database), enabling powerful hierarchical agent swarms.
*   **Safeguards**: For non-macro steps (like `command` or `set`), the system enforces synchronous execution if an active transaction exists to prevent accidental data corruption.
*   **Error Propagation**: By default, if an async step fails, it cancels the entire group. You can override this with `"continue_on_error": true`.

### 3. Transaction Inheritance (Subroutines)
If a nested macro is run **synchronously** (`"is_async": false`) and does **not** specify a different database, it acts as a **Subroutine**.
*   **Inheritance**: It inherits the parent's active transaction.
*   **Atomicity**: Operations in the subroutine are part of the parent's atomic unit of work. If the subroutine fails, the parent transaction rolls back.

### 4. Saga Pattern (Multi-Database Workflows)
SOP encourages the **Saga Pattern** for workflows that span multiple databases. Instead of a single distributed transaction (which is complex and brittle), you chain macros where each macro operates on a specific database.

*   **Database-Scoped Macros**: A macro can specify a `"database"` field.
*   **Automatic Transaction Wrapping**: When a macro specifies a database, the system automatically:
    1.  Switches the context to that database.
    2.  Starts a new transaction (`ForWriting`).
    3.  Commits the transaction if the macro succeeds.
    4.  Rolls back if it fails.

### 4. Tool Registry
Tools are no longer hardcoded strings. They are defined in a structured `Registry`.
*   **Definition**: Tools have a Name, Description, Argument Schema, and a Go Handler function.
*   **Discovery**: Agents can dynamically list and inspect available tools to generate their system prompts.

### 5. Explicit Transaction Management
While automatic transaction wrapping (Saga Pattern) is recommended for most cases, you can still manage transactions explicitly using the `manage_transaction` tool. This is useful when:
*   **Minimizing Lock Time**: You want to perform heavy AI processing or external API calls *outside* of a transaction, and only wrap the database writes.
*   **Multiple Transactions**: A single macro needs to perform multiple independent commits.
*   **Inherited Context**: You are running a macro without a specific `database` field and want to control the transaction on the inherited database connection.

---

## Macro Schema

A `Macro` consists of metadata and a list of `Steps`.

```json
{
  "name": "example_macro",
  "description": "Demonstrates async and db features",
  "parameters": ["user_id"],
  "database": "users_db", // Optional: Target DB for this macro
  "steps": [ ... ]
}
```

### Step Types

| Type | Description | Key Fields |
| :--- | :--- | :--- |
| `ask` | Ask the LLM a question | `prompt`, `output_variable` |
| `set` | Set a variable | `variable`, `value` |
| `if` | Conditional logic | `condition`, `then`, `else` |
| `loop` | Iterate over a list | `list`, `iterator`, `steps` |
| `fetch` | Retrieve data from B-Tree | `source`, `resource`, `variable` |
| `command` | Execute a registered tool | `command`, `args` |
| `macro` | Run a nested macro | `macro_name`, `macro_args` |

### Async & Error Handling Fields

*   `"is_async": true`: Runs the step in a background goroutine.
*   `"continue_on_error": true`: If this step fails, do not stop the rest of the macro.

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

Orchestrate a workflow across two databases using nested macros.

**Macro 1: Update User (on `users_db`)**
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

**Macro 2: Log Audit (on `audit_db`)**
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

**Macro 3: Orchestrator (Saga)**
```json
{
  "name": "user_update_saga",
  "steps": [
    {
      "type": "macro",
      "macro_name": "update_user",
      "macro_args": {"uid": "123"}
    },
    {
      "type": "macro",
      "macro_name": "log_audit",
      "macro_args": {"action": "User 123 updated"}
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
