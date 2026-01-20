# AI Copilot Scripts: Natural Language Programming

The SOP AI Copilot includes a powerful **Script System** that transforms your AI chat sessions into a "Natural Language Programming" environment. It allows you to draft, refine, and execute complex workflows that combine the flexibility of LLMs with the performance of compiled code.

## Concept: "Compiled Instructions"

SOP Scripts are **deterministic programs**.
*   **Drafting**: You create scripts interactively by adding steps via chat commands.
*   **Execution**: The SOP engine executes these steps directly using compiled Go code. This is akin to running a compiled binary or a high-performance stored procedure.

## The SOP Advantage: Bare Metal Performance

SOP exposes its B-Tree engine directly to the script system. This effectively creates a **machine instruction set** for end-users.

*   **Bare Metal API**: SOP's B-Tree API is compiled Go code.
*   **High-Performance Units**: Scripts are sequences of JSON instructions that command this compiled code to perform iterations, expression evaluations, and data lookups.
*   **No Parsing Overhead**: Unlike SQL statements that require complex parsing and query planning at runtime, SOP scripts are pre-structured units of work.
*   **System Database**: Scripts are persisted in a dedicated **SystemDB**, a robust SOP B-Tree store. This ensures your "programs" are durable, transactional, and available across server restarts.

## Tools Reference

The SOP AI Copilot exposes a comprehensive set of tools for managing scripts and data.

### Script Management

| Tool | Description | Arguments |
| :--- | :--- | :--- |
| `list_scripts` | Lists all available scripts. | `()` |
| `save_script` | Saves a full script definition (create or overwrite). Use this to persist a script after drafting or when importing a valid JSON script definition. | `(name: string, description: string, steps: list<object>)` |
| `get_script_details` | Retrieves the definition of a specific script. | `(name: string)` |
| `save_step` | Appends a new step to a specific script. | `(script: string, ...step_def)` |
| `insert_step` | Inserts a step into a script at a specific index. | `(script: string, index: number, type: string, description: string, name: string, ...params)` |
| `delete_step` | Deletes a step from a script. | `(script: string, index: number)` |
| `update_step` | Updates an existing step in a script. | `(script: string, index: number, description: string, name: string, ...params)` |
| `reorder_steps` | Moves a step to a new position. | `(script: string, from_index: number, to_index: number)` |
| `add_step_from_last` | Adds the last executed tool call as a new step. | `(script: string, index: number, position: string, description: string, name: string)` |
| `refactor_last_interaction` | Refactors the immediate past interaction into a new script. | `(mode: string, name: string)` |

### Data Operations

| Tool | Description | Arguments |
| :--- | :--- | :--- |
| `list_databases` | Lists all available databases. | `()` |
| `list_stores` | Lists all stores in the current database. | `(database: string (optional))` |
| `select` | Standard data retrieval with filtering and sorting. | `(store: string, filter: object, limit: number, skip: number, order_by: list, fields: list)` |
| `add` | Inserts a single record. | `(store: string, key: any, value: any)` |
| `update` | Updates a single record. | `(store: string, key: any, value: any)` |
| `delete` | Removes a single record. | `(store: string, key: any)` |
| `execute_script` | Executes a raw, multi-step programmatic script (JSON execution plan). | `(script: list<op>)` |

## Session Commands

When interacting with the AI Copilot interactively (e.g., in the web console), you can use slash commands to manage your session, data, and draft scripts manually. This gives you a full CLI experience.

### Data Operations (CLI)

| Command | Usage | Description |
| :--- | :--- | :--- |
| `/select` | `/select <store> [limit=N]` | List top N records from a store. Defaults to limit=10. |
| `/add` | `/add <store> <key> <value>` | Add or update a record in the store. Value can be a JSON string or plain text. |
| `/update` | `/update <store> <key> <value>` | Update an existing record. Fails if key not found. |
| `/delete_record` | `/delete_record <store> <key>` | Delete a specific record from the store. |

### Script Management (CLI)

| Command | Usage | Description |
| :--- | :--- | :--- |
| `/create` | `/create <name> [goal]` | Start recording a new script draft. |
| `/list` | `/list [--category <cat>]` | Lists all available scripts. |
| `/show` | `/show <name> [--json] `--category <cat>` | Displays the content of a saved script. |
| `/save` | `/save` | Saves the current draft to the System DB. |
| `/save_as` | `/save_as <name>` | Saves the last executed tool call as a new script immediately. |
| `/delete` | `/delete <name>` | Deletes a saved script. |
| `/step` | `/step <instruction>` | Adds a step to the current draft. If text follows, it adds a prompt step. If empty, it adds the last executed command. |
| `/insert_step` | `/insert_step <script> <idx>` | Inserts the last executed command into an *existing* script. |
| `/update_step` | `/update_step <idx> <instruction>` | Updates the instruction of a specific step in current draft. |
| `/delete_step` | `/delete_step <idx>` | Removes a specific step from current draft. |
| `/reorder_steps` | `/reorder_steps <old> <new>` | Moves a step from one position to another in current draft. |
| `/refine` | `/refine [instruction]` | Uses AI to improve a script draft. |

## Atomic Lego Blocks: The Foundation of Safe Scripting

To enable the LLM to generate scripts with "fine-grained agility" and control, we have exposed a set of **Atomic Lego Blocks**. These are highly optimized, safe, and versatile functions that the LLM can assemble to perform complex logic without writing raw code.

*   **`compare(val1, val2)`**: A universal comparator that intelligently handles strings, numbers, and dates. It returns `-1`, `0`, or `1`, making it perfect for sorting and range checks.
*   **`matchesMap(item, criteria)`**: A powerful pattern matcher inspired by MongoDB query syntax. It supports operators like:
    *   `$gt`, `$gte`, `$lt`, `$lte`: Range comparisons.
    *   `$in`: Set membership.
    *   `$eq`, `$ne`: Equality checks.
    *   `$regex`: Pattern matching.
*   **`toFloat(val)`**: A robust type converter that safely extracts numerical values from various input types (strings, ints, floats) for mathematical operations.
*   **`Scan(store, options)`**: The fundamental B-Tree traversal block. It supports forward/backward iteration, prefix matching, and range queries, serving as the engine for `SELECT * FROM ... WHERE ...` style operations.
*   **`JoinRightCursor(left_store, right_store, join_key)`**: A specialized cursor that efficiently performs a Right Outer Join between two B-Trees. It iterates through the 'Right' store and performs optimized lookups in the 'Left' store, handling missing matches gracefully.

### Variable Resolution & Join Semantics

When using operations like `Join` or `Project`, the script engine manages variable scope intelligently.

*   **Automatic Aliasing**: When joining on a Store Variable (e.g. a store opened with `scan`), the engine automatically sets the alias to the Store Name (e.g. `users`). This allows for intuitive projections like `{"users.*": 1}` without needing to manually specify an alias.
*   **Strict vs. Loose Resolution**:
    *   **Strict Mode** (Internal): Variables are fully qualified (e.g., `storeName.fieldName`). This avoids ambiguity and is how the engine defaults when handling Store Variables.
    *   **Loose Mode** (External): In your scripts (where ambiguity is low), you can refer to fields directly (e.g. `age` instead of `users.age`). The engine resolves this to the correct source automatically. To force a specific source or handle name collisions, use the strict `storeName.field` syntax.

**Why this matters:**
Instead of asking the LLM to "write a Python script to filter users," we ask it to "generate a JSON structure using `matchesMap`." This ensures:
1.  **Safety**: No arbitrary code execution.
2.  **Consistency**: The logic behaves exactly the same way every time, powered by our compiled Go implementation.
3.  **Agility**: The LLM can easily tweak the JSON parameters (e.g., changing a threshold from `> 10` to `> 20`) without rewriting logic.

## Streaming & Minimal Memory Footprint

A critical advantage of using `Scan` and `JoinRightCursor` is **Streaming**.

*   **Zero-Buffer Execution**: When performing a Join or a large Scan, the system does *not* load all results into memory. Instead, it streams items one by one through the pipeline.
*   **Huge Volume Support**: You can perform complex SQL-style Joins on B-Trees containing millions of records with **minimal memory requirements**. The memory footprint remains constant regardless of dataset size.
*   **Remote Agility**: These streams are piped directly to the HTTP response, allowing a REST client on the other side of the world (or the UI) to start receiving data immediately, byte-by-byte.

## Hybrid Execution Model: LLM vs. Code

SOP Scripts function as a hybrid engine, allowing you to mix **Intelligence** with **Performance**.

1.  **Natural Language Steps (`ask`)**
    *   **What it is**: A prompt for the LLM.
    *   **Behavior**: The system pauses, sends the context to the LLM, and executes the response.
    *   **Use Case**: Decision making, analyzing text, reasoning about data.
    *   **Example**: `/step Analyze the sentiment of the last 5 reviews.`

2.  **Tool Steps (`command`)**
    *   **What it is**: A precise instruction for the SOP Engine.
    *   **Behavior**: **Direct Execution**. The LLM is **skipped entirely**.
    *   **Use Case**: Deterministic data fetching, math, standardized business logic.
    *   **Example**: `Scan(store="Users", query={"age": {"$gt": 18}})` -> *Runs in microseconds.*

**The "Drafting" Power Move:**
When you ask the AI to "Find users in California", it runs a tool. If you then type `/step`, you save that **Tool Step** to your script.
*   **Result**: You used the LLM to *write* the code, but the final script runs as *pure code*.

## Workflow: Drafting & Execution

We have replaced "Record & Play" with a more robust **Drafting** workflow. This allows you to construct scripts intentionally, avoiding the noise of conversation.

### 1. Drafting a Script
*   **Start**: `/create <name> [category] [--autosave]` initiates a draft.
    *   **--autosave**: Optional. Automatically saves the script to the database after every `/step` command, ensuring no work is lost during long drafting sessions.
*   **Add Logic**:
    *   **Manual**: `/step <instruction>` adds a natural language instruction to the script.
    *   **From History**: `/step` (with no args) adds the *last executed command* to the script. This allows you to test a command and then "commit" it to the script.
*   **Save**: `/save` persists the draft to the SystemDB (useful as a final checkpoint or if auto-save is off).

### 2. Execution
*   **Run**: `/run <name> [param=value ...]` executes the script.
    *   **Parameters**: Pass variables like `region=US` or `limit=10`.
    *   **Context**: The script runs in the context of your current database session unless specified otherwise.

## Script Schema (The "Language")

The script system uses a stable JSON schema acting as a mini-SDK. Each step now includes a **Name** (ID) and **Description** (Docstring) to make scripts self-documenting and easier for looking up logic.

### Step Structure
```json
{
  "name": "fetch_user_data",
  "description": "Retrieve the user record from the primary database",
  "type": "tool",
  "function": "fetch_record",
  "args": { ... }
}
```

### Core Operations
*   **`ask`**: Query the LLM for reasoning or creative generation.
*   **`tool`**: Execute any registered tool (database ops, file IO (safe), etc).
*   **`set`**: Assign values to variables.
*   **`if`**: Conditional branching logic.
*   **`loop`**: Iterate over lists or data fetched from B-Trees.
*   **`fetch`**: High-performance data retrieval directly from SOP B-Trees.
*   **`say`**: Output information to the user.

## Advanced Orchestration

### Transaction Management
Scripts can manage data integrity just like trusted backend code.
*   **Implicit Transactions**: By default, scripts run in a unified context.
*   **Explicit Control**: Scripts can utilize transaction tools (e.g., `begin_transaction`, `commit_transaction`) to handle complex multi-step commits, ensuring that a chain of operations either succeeds entirely or rolls back safely.

### Map/Reduce & Batch Processing
The combination of `loop` and `tool` steps enables a Map/Reduce pattern:
1.  **Map**: Use `fetch` or `Scan` to retrieve a cursor of items.
2.  **Reduce**: Use `loop` to iterate over the cursor, applying a `tool` (like `update_record` or `calculate_metric`) to each item.
This allows the agent to perform massive batch operations (e.g., "Update the status of all 10,000 overdue orders") efficiently and reliably.

## Remote Execution via REST API

Because scripts are stored in the persistent SystemDB, they are instantly available as **API Endpoints**. You can trigger any complex workflow remotely:

```bash
curl -X POST http://localhost:8080/api/ai/chat \
  -d '{"message": "/play daily_report date=2025-01-01", "agent": "sql_admin"}'
```

This turns your SOP server into a programmable application server where business logic can be defined via chat and executed via REST.

## Example Workflow

1.  **Define Logic (Chat or JSON)**:
    ```json
    {
      "name": "process_users",
      "steps": [
        { "type": "fetch", "source": "btree", "resource": "users", "variable": "user_list" },
        { 
          "type": "loop", "list": "user_list", "iterator": "user", 
          "steps": [
            { "type": "ask", "prompt": "Is {{.user.name}} a VIP? (yes/no)", "output_variable": "is_vip" },
            { 
              "type": "if", "condition": "{{eq .is_vip \"yes\"}}",
              "then": [{ "type": "say", "message": "Sending gift to {{.user.name}}" }]
            }
          ]
        }
      ]
    }
    ```
2.  **Execute**:
    ```
    /play process_users
    ```
    *The system fetches 1000 users from the B-Tree (in milliseconds), loops through them in Go, and only uses the LLM to decide VIP status.*

In other words, we have turned the AI chatbot into an **IDE**, where users or data administrators can create and execute compiled program units, akin to Stored Procedures but with the reasoning power of an LLM.
