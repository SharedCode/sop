# Execute Script Tool
Use the `execute_script` tool to programmatically interact with SOP databases. This tool uses a Native JSON execution engine, meaning you must provide a valid JSON array of Abstract Syntax Tree (AST) step objects.

This tool is required for complex multi-step atomic operations, pipelined data processing (scans, joins, filters, projections), and cross-store data manipulation.

<h2> JSON Tool Calling Signature</h2>
When the Native LLM orchestrator invokes this tool, you must supply the AST array to the `script` parameter field!
```json
{
  "name": "execute_script",
  "args": {
    "script": [
      { "op": "...", "args": {...} }
    ]
  }
}
```

<h2> AST Step Object Schema</h2>
Every step in the script array is a JSON object that strictly follows this format:
- `op` (string, required): The operation to perform (e.g., "open_db", "scan").
- `args` (object, optional): A dictionary of arguments specific to the `op`.
- `input_var` (string, optional): The variable name to read input from (useful for pipeline chaining).
- `result_var` (string, optional): The variable name to store the output into for subsequent steps.

<h2> Supported Operations & Expected `args`</h2>
*   **`open_db`**: `{"name": "string"}` -> db. Use this only when you must switch databases explicitly. If a Current Database is already active in context, prefer omitting `open_db`; otherwise set `name` to that active database instead of inventing a database name.
*   **`begin_tx`**: `{"database": "string", "mode": "read" | "write"}` -> tx
*   **`commit_tx`**: `{"transaction": "string"}` 
*   **`rollback_tx`**: `{"transaction": "string"}`
*   **`open_store`**: `{"transaction": "string", "name": "string"}` -> store
*   **`add`**: `{"store": "string", "key": any, "value": any}` -> inserts a new record
*   **`find`**: `{"store": "string", "key": any}` -> seeks to a record by key
*   **`get_current_value`**: `{"store": "string"}` -> gets the value of the found/focused record
*   **`scan`**: `{"store": "string", "limit": number, "direction": "asc" | "desc", "start_key": any, "prefix": string, "filter": object, "stream": true}` -> cursor
*   **`sort`**: `{"fields": ["string"]} (e.g. "age desc")` -> list
*   **`filter`**: `{"condition": "string"}` -> cursor/list
*   **`project`**: `{"fields": ["string"]}` -> cursor/list
*   **`limit`**: `{"limit": number}` -> cursor/list
*   **`join`**: `{"with": "string", "type": "inner"|"left"|"right", "on": object}` -> cursor/list
*   **`join_right`**: `{"store": "string", "on": object}` -> cursor/list (Pipeline alias for join)
*   **`update`**: `{"store": "string"}` -> bulk updates from piped list
*   **`delete`**: `{"store": "string"}` -> bulk deletes from piped list
*   **`return`**: `{"value": any}` -> gracefully halting early

<h2> Few-Shot Example: Querying, Joining, and Filtering</h2>
When pipelining data, chain variables using `result_var` on step N and `input_var` on step N+1.

```json
[
  {"op": "open_db", "args": {"name": "mydb"}},
  {"op": "begin_tx", "args": {"database": "mydb", "mode": "read"}, "result_var": "tx1"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "users"}, "result_var": "users_store"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "orders"}, "result_var": "orders_store"},
  
  {"op": "scan", "args": {"store": "users_store", "stream": true}, "result_var": "user_stream"},
  
  {"op": "join_right", "args": {"store": "orders_store", "on": {"user_id": "user_id"}}, "input_var": "user_stream", "result_var": "joined_stream"},
  
  {"op": "filter", "args": {"condition": "age > 30"}, "input_var": "joined_stream", "result_var": "filtered_stream"},
  
  {"op": "project", "args": {"fields": ["name", "order_date"]}, "input_var": "filtered_stream", "result_var": "projected"},
  
  {"op": "limit", "args": {"limit": 5}, "input_var": "projected", "result_var": "output"},
  
  {"op": "commit_tx", "args": {"transaction": "tx1"}}
]
```

<h2> Best Practices</h2>
- **Tool Choice Discipline**: Do not call `list_tools` for ordinary store/database requests. The relevant Stores operations are already injected in this context. Use `list_tools` only if the user explicitly asks about available tools or capabilities.
- **Database Choice Rule**: The active Current Database from context is the default database. Prefer `begin_tx` directly on that database. If you still emit `open_db`, use the active database name exactly as provided by context.
- **Strict AST Sequencing**: When you explicitly switch databases, open the DB (`open_db`), then begin a transaction (`begin_tx`), then open any required data stores (`open_store`) before you can scan them.
- **Projection Order for UX:** When a user requests data to be sorted or filtered by a specific field, always ensure that the operation `project` places that specific field as the **first item** in the `fields` array. This makes the sorting/filtering immediately obvious to the user in the resulting output.
- **Relational Joins:** The dynamic Context payload will supply `Relations: [foreign_key] -> target_store([primary_key])`. Look at these carefully when invoking `join` or `join_right`; your `on` condition must match these constraints exactly to map entities successfully.

