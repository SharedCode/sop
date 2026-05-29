# Execute Script Tool
Use `execute_script` for multi-step store operations. Provide a JSON array of AST steps in `args.script`.

<h2> Core Conventions</h2>
- Use `result_var` and `input_var` to chain multi-step reads.
- Prefer `begin_tx` on the active Current Database unless a database switch is needed. If the active database is already correct, omit `open_db`.
- Read flows usually look like `begin_tx(mode=read)` -> `open_store` -> `scan/find/filter/project/sort/limit/join` -> `commit_tx` or `rollback_tx` -> `return`.
- Write flows usually look like `begin_tx(mode=write)` -> `open_store` -> `add/update/delete` -> `commit_tx` or `rollback_tx`.
- Use concrete predicate objects such as `{"first_name":{"$eq":"John"}}`, not placeholder booleans or nulls.

<h2> Research & Orchestration Rules</h2>
- Use `list_stores` to research schema and relations when field names, value types, predicate shapes, or join mappings are ambiguous.
- `list_stores` accepts a `stores` parameter containing the exact store names you want to research.
- Pass `stores: ["users", "users_orders", "orders"]` to `list_stores` when you only need a few stores.
- Prefer scoped `stores: [...]` lookups over listing the whole database when the ask already suggests likely target stores; this keeps research compact and avoids scale issues.
- `list_stores` returns one grounded line per store, for example `users schema={...}` or `orders schema={...} relations=[...]`.
- Read `schema=...` to get exact field names and likely value types.
- Read `relations=[...]` to get relationship semantics: which store is related, and which source/target fields define the join path.
- Treat `relations=[...]` from `list_stores` as the source of truth for related stores and join key mapping details.
- For multi-store reads, call `list_stores` first, inspect the returned `schema=...` and `relations=...` lines, then build `execute_script` from those grounded names instead of guessing.
- Use `gettoolinfo('execute_script')` only when the AST shape itself is unclear.
- Keep the script focused on orchestration: begin a transaction, read or mutate stores, then commit or rollback.

<h2> Example</h2>
Use `result_var` and `input_var` to chain steps. Use field paths that match the relation mapping you researched.

```json
[
  {"op": "begin_tx", "args": {"mode": "read"}, "result_var": "tx"},
  {"op": "open_store", "args": {"transaction": "tx", "name": "users"}, "result_var": "users"},
  {"op": "open_store", "args": {"transaction": "tx", "name": "users_orders"}, "result_var": "users_orders"},
  {"op": "scan", "args": {"store": "users", "stream": true, "filter": {"users.first_name": {"$eq": "John"}}}, "result_var": "user_stream"},
  {"op": "join_right", "args": {"store": "users_orders", "on": {"users.key": "key"}}, "input_var": "user_stream", "result_var": "users_with_order_refs"},
  {"op": "filter", "args": {"condition": {"users_orders.value": {"$eq": "o1"}}}, "input_var": "users_with_order_refs", "result_var": "output"},
  {"op": "commit_tx", "args": {"transaction": "tx"}},
  {"op": "return", "args": {"value": {"$var": "output"}}}
]
```

