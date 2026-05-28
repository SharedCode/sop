# Execute Script Tool
Use `execute_script` for multi-step store operations. Provide a JSON array of AST steps in `args.script`.

<h2> Tool Call Shape</h2>
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
Each step is a JSON object with this shape:
- `op` (string, required): The operation to perform (e.g., "open_db", "scan").
- `args` (object, optional): A dictionary of arguments specific to the `op`.
- `input_var` (string, optional): The variable name to read input from (useful for pipeline chaining).
- `result_var` (string, optional): The variable name to store the output into for subsequent steps.

<h2> Supported Operations & Expected `args`</h2>
*   **`open_db`**: `{"name": "string"}` -> db. Prefer omitting this when the active Current Database is already correct.
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
*   **`join_right`**: `{"store": "string", "on": object}` -> cursor/list
*   **`update`**: `{"store": "string"}` -> bulk updates from piped list
*   **`delete`**: `{"store": "string"}` -> bulk deletes the piped records from a store (`delete records`).
*   **`return`**: `{"value": any}` -> gracefully halting early

<h2> Deletion Operations</h2>

Map the user's phrasing to the correct operation:

*   **If the user says `delete record`, `delete data`, or `delete row`**
  * Use record deletion inside the store.
  * Direct tool syntax: `delete(store: string, key: any)`
  * AST syntax: identify records first, then pipe them into `delete`.
  * Example AST:

```json
[
  {"op": "begin_tx", "args": {"mode": "write"}, "result_var": "tx"},
  {"op": "open_store", "args": {"transaction": "tx", "name": "users"}, "result_var": "users_store"},
  {"op": "scan", "args": {"store": "users_store", "filter": {"status": {"$eq": "inactive"}}}, "result_var": "inactive_users"},
  {"op": "delete", "args": {"store": "users_store"}, "input_var": "inactive_users"},
  {"op": "commit_tx", "args": {"transaction": "tx"}}
]
```

*   **If the user says `delete store`, `delete the users store`, or `drop store users`**
  * Use store deletion.

<h2> Example</h2>
Use `result_var` and `input_var` to chain steps.

If `open_store` uses `"result_var": "users"`, prefer reusing `users` later in the script.
Use prefixed field paths consistently in join, filter, sort, and project steps, such as `users.key`, `users.first_name`, `users_orders.value`, and `orders.total_amount`.

```json
[
  {"op": "open_db", "args": {"name": "mydb"}, "result_var": "db"},
  {"op": "begin_tx", "args": {"database": "db", "mode": "read"}, "result_var": "tx"},
  {"op": "open_store", "args": {"transaction": "tx", "name": "users"}, "result_var": "users"},
  {"op": "open_store", "args": {"transaction": "tx", "name": "users_orders"}, "result_var": "users_orders"},
  {"op": "open_store", "args": {"transaction": "tx", "name": "orders"}, "result_var": "orders"},
  
  {"op": "scan", "args": {"store": "users", "stream": true, "filter": {"users.first_name": {"$eq": "John"}}}, "result_var": "user_stream"},
  
  {"op": "join_right", "args": {"store": "users_orders", "on": {"users.key": "key"}}, "input_var": "user_stream", "result_var": "users_with_order_refs"},
  
  {"op": "join_right", "args": {"store": "orders", "on": {"users_orders.value": "key"}}, "input_var": "users_with_order_refs", "result_var": "joined_stream"},
  
  {"op": "filter", "args": {"condition": {"orders.total_amount": {"$gt": 500}}}, "input_var": "joined_stream", "result_var": "filtered_stream"},
  
  {"op": "project", "args": {"fields": ["users.first_name", "orders.key AS order_id", "orders.total_amount", "orders.order_date"]}, "input_var": "filtered_stream", "result_var": "output"},
  
  {"op": "limit", "args": {"limit": 5}, "input_var": "output", "result_var": "output"},
  
  {"op": "commit_tx", "args": {"transaction": "tx"}},
  {"op": "return", "args": {"value": {"$var": "output"}}}
]
```

<h2> Best Practices</h2>
- Prefer `begin_tx` on the active Current Database unless a database switch is needed.
- Open stores before scanning them.
- Reuse the same `result_var` for a store handle throughout the script.
- When sorting or filtering for a user-facing result, put the key field first in `project.fields`.
- Use relation metadata from context when building joins.

