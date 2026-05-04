# Execute Script Tool
The Execute Script tool provides a programmable natural language interface to the SOP B-Tree system.

## execute_script Tool Operations:
- open_db(name) -> db
- begin_tx(database, mode) -> tx
- commit_tx(transaction)
- rollback_tx(transaction)
- open_store(transaction, name) -> store
- scan(store, limit, direction ("asc" or "desc"), start_key, prefix, filter, stream=true) -> cursor
- sort(input, fields) -> list
- filter(input, condition) -> cursor/list
- project(input, fields) -> cursor/list
- limit(input, limit) -> cursor/list
- join(input, with, type, on) -> cursor/list
- join_right(input, store, type, on) -> cursor/list (Pipeline alias for join)
- update(input, store) -> bulk updates the incoming piped list of records in the store
- delete(input, store) -> bulk deletes the incoming piped list of records from the store
- if(condition, then, else)
- loop(condition, body)
- call_script(name, params)
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

## Best Practices
- **Projection Order for UX:** When a user requests data to be sorted or filtered by a specific field (e.g., "sorted by Age descending"), always ensure that the operation `project` places that specific field as the **first item** in the `fields` array (e.g., `["users.age", "users.key", ...]`). This makes the sorting/filtering immediately obvious to the user in the resulting output.
