# Execute Script Tool
The Execute Script tool provides a programmable natural language interface to the SOP B-Tree system.

## execute_script Tool Instruction
Execute a programmatic script to interact with databases. Use this for complex multi-step operations not covered by high-level tools. Supports variables, transaction management, B-Tree cursor navigation, and memory list manipulation.

Important:
1. Inspect Schema First: Use 'list_stores' to discover stores and their schema, use field names when referencing, e.g. writing projection fields, filtering logic.
2. Inspect the "relations" fields of the store schemas to determine the correct join logic and optimized access paths.
3. When joining using a Secondary Index or KV store, respect the field names in the 'Relation'. If a Relation maps '[Value]' to 'target_id', use 'Value' in your 'on' clause (e.g. {"on": {"Value": "target_id"}}). Do not assume the source has the target's field name.
4. 'scan' and 'join' return full objects. To select specific fields or renamed columns, you MUST add a 'project' step. The 'fields' list defines the EXACT order of keys in the output JSON. You MUST respect the user's requested SELECT clause order. If the user asks for "all entities" (e.g. "all users"), prioritize projecting "store.*" (e.g. ["users.*"]).
5. For large datasets, prefer using 'limit' to avoid memory exhaustion.
6. Store names are sometimes entity's plural form or singular form, e.g. user entity stored in users store.
7. Field names sometimes use underscore('_') separator instead of space(' '), e.g. - "total amount" as field name is "total_amount".
8. Group Atomic Operations: Partial or atomic data operations (scan, filter, join, project) should be grouped into a single 'execute_script' block. Do not create separate tool calls or script steps for these unless user interaction is required in between. This ensures transactional safety and performance.

Operations:
- open_db(name) -> db
- begin_tx(database, mode) -> tx
- commit_tx(transaction)
- rollback_tx(transaction)
- open_store(transaction, name) -> store
- scan(store, limit, direction ("asc" or "desc"), start_key, prefix, filter, stream=true) -> cursor
- find(store, key, desc) -> bool
- next(store) -> bool
- previous(store) -> bool
- first(store) -> bool
- last(store) -> bool
- get_current_key(store) -> key
- get_current_value(store) -> value
- add(store, key, value)
- update(store, key, value)
- delete(store, key)
- list_new() -> list
- list_append(list, item)
- map_merge(map1, map2) -> map
- sort(input, fields) -> list
- filter(input, condition) -> cursor/list
- project(input, fields) -> cursor/list (fields: list<string> ['field', 'field AS alias', 'a.*'] (PREFERRED). Order is guaranteed and critical for SELECT queries., or map {alias: field} (no ordering guaranteed))
- limit(input, limit) -> cursor/list
- join(input, with, type, on) -> cursor/list
- join_right(input, store, type, on) -> cursor/list (Pipeline alias for join)
- if(condition, then, else)
- loop(condition, body)
- call_script(name, params)
- select(store, key, value, fields, limit, order_by, action, update_values) -> list (high-level tool integration)
- return(value) -> stops execution and returns value

Example Pipeline Join:
[
  {"op": "open_db", "args": {"name": "mydb"}},
  {"op": "begin_tx", "args": {"database": "mydb", "mode": "read"}, "result_var": "tx1"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "users"}, "result_var": "users"},
  {"op": "open_store", "args": {"transaction": "tx1", "name": "orders"}, "result_var": "orders"},
  {"op": "scan", "args": {"store": "users", "stream": true}, "result_var": "stream"},
  {"op": "join_right", "args": {"store": "orders", "on": {"user_id": "user_id"}}, "input_var": "stream", "result_var": "stream"},
  {"op": "project", "args": {"fields": ["name", "order_date"]}, "input_var": "stream", "result_var": "projected"},
  {"op": "limit", "args": {"limit": 5}, "input_var": "projected", "result_var": "output"},
  {"op": "commit_tx", "args": {"transaction": "tx1"}}
]
Note: 'scan' and 'join' return full objects. To select specific fields or renamed columns, you MUST add a 'project' step.