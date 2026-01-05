# SOP Scripting Engine Guide

This guide describes the **Object-Oriented Scripting Language** used by the Data Admin Agent to interact with SOP databases. This language allows the LLM to compose complex workflows, optimize queries, and manage transactions programmatically.

## Core Concepts

The scripting engine is based on **Traits** and **Capabilities**. The LLM acts as a "Query Optimizer," choosing the best combination of operations based on the capabilities of the underlying objects.

### Objects & Traits

1.  **B-Tree Source (`store`)**
    *   **Trait**: **Ordered Storage**.
    *   **Capability**: Efficiently scan data in **Ascending** or **Descending** order ($O(\log N)$).
    *   **Constraint**: Can only sort by the **Index Key**.
    *   **Operations**: `scan`, `inspect`.

2.  **Memory Processor (`list`)**
    *   **Trait**: **Flexible Manipulation**.
    *   **Capability**: Can Filter, Sort, Project, and Limit data in arbitrary ways.
    *   **Constraint**: Memory-bound. Operations are $O(N)$ or $O(N \log N)$.
    *   **Operations**: `filter`, `sort`, `project`, `limit`.

3.  **Transaction (`tx`)**
    *   **Trait**: **ACID Boundary**.
    *   **Capability**: Ensures atomicity of multiple operations.
    *   **Operations**: `begin_tx`, `commit_tx`, `rollback_tx`.

---

## SQL-to-Script Mapping Examples

### Scenario 1: Mixed Sorting (The "Impossible" Query)
**User Ask**: "Show me employees sorted by Region DESC, then Department ASC."

**Analysis**:
*   B-Tree Key: `Region` (Primary).
*   Problem: B-Tree can scan `Region` DESC, but `Department` will be secondary sorted (likely DESC or random depending on key structure).
*   Solution: Use B-Tree for the heavy lifting (Region DESC), then Memory Sort for the fine-tuning (Department ASC).

**Script**:
```json
[
  { "op": "open_db", "args": { "name": "HR" }, "result_var": "db" },
  { "op": "begin_tx", "args": { "database": "db" }, "result_var": "tx" },
  { "op": "open_store", "args": { "transaction": "tx", "name": "employees" }, "result_var": "s" },
  
  // 1. B-Tree Trait: Efficient Descending Scan
  // This gets us 90% of the way there (Region is correct).
  { "op": "scan", "args": { "store": "s", "direction": "desc", "limit": 1000 }, "result_var": "raw_data" },
  
  // 2. Memory Trait: Flexible Multi-Field Sort
  // This fixes the secondary order (Department ASC) while preserving Region DESC.
  { "op": "sort", "input_var": "raw_data", "args": { "fields": ["key.region desc", "key.department asc"] }, "result_var": "sorted_data" },
  
  { "op": "assign", "input_var": "sorted_data", "result_var": "output" },
  { "op": "commit_tx", "args": { "transaction": "tx" } }
]
```

### Scenario 2: "Update if Exists" (Read-Modify-Write)
**User Ask**: "Give a 10% raise to everyone in IT."

**Analysis**:
*   Need to find IT staff (Filter).
*   Need to calculate new salary (Project/Update).
*   Need to save back (Update).

**Script**:
```json
[
  { "op": "open_db", "args": { "name": "HR" }, "result_var": "db" },
  { "op": "begin_tx", "args": { "database": "db", "mode": "write" }, "result_var": "tx" },
  { "op": "open_store", "args": { "transaction": "tx", "name": "employees" }, "result_var": "s" },
  
  // 1. Scan all (or prefix if 'dept' was the key)
  { "op": "scan", "args": { "store": "s" }, "result_var": "all" },
  
  // 2. Filter in Memory
  { "op": "filter", "input_var": "all", "args": { "condition": { "value.dept": "IT" } }, "result_var": "it_staff" },
  
  // 3. Bulk Update
  // Note: 'update' operation merges values. For calculation, we might need a 'map' op later.
  // For now, we assume the user provides the static value or we implement a 'calc' op.
  // Assuming we just set a flag for this example.
  { "op": "update", "input_var": "it_staff", "args": { "store": "s", "values": { "raise_given": true } }, "result_var": "updated_count" },
  
  { "op": "commit_tx", "args": { "transaction": "tx" } }
]
```

### Scenario 3: Introspection & Optimization
**User Ask**: "Find users with age > 20."

**Analysis**:
*   LLM doesn't know if `age` is a key.
*   Step 1: Inspect.

**Script**:
```json
[
  { "op": "open_db", "args": { "name": "UsersDB" }, "result_var": "db" },
  { "op": "begin_tx", "args": { "database": "db" }, "result_var": "tx" },
  { "op": "open_store", "args": { "transaction": "tx", "name": "users" }, "result_var": "s" },
  
  // 1. Inspect to see capabilities
  { "op": "inspect", "args": { "store": "s" }, "result_var": "info" },
  
  // LLM Logic (Internal):
  // If info.key_type == "age", I can use scan(start_key=20).
  // If info.key_type != "age", I must use scan() + filter().
  
  { "op": "assign", "input_var": "info", "result_var": "output" },
  { "op": "commit_tx", "args": { "transaction": "tx" } }
]
```

## Operation Reference

| Operation | Input | Args | Result | Description |
| :--- | :--- | :--- | :--- | :--- |
| `open_db` | - | `name` | `Database` | Opens a database connection. |
| `begin_tx` | - | `database`, `mode` | `Transaction` | Starts a transaction (`read` or `write`). |
| `open_store` | - | `transaction`, `name` | `Store` | Opens a B-Tree store. |
| `scan` | - | `store`, `limit`, `direction`, `start_key`, `prefix`, `filter` | `List` | Scans the B-Tree. Uses `Find` for `start_key`. Applies `filter` (CEL) during scan (Push Down). |
| `filter` | `List` | `condition` (Map) | `List` | Filters items in memory. |
| `sort` | `List` | `fields` (Array) | `List` | Sorts items in memory. Supports `["field desc"]`. |
| `project` | `List` | `fields` (Array) | `List` | Selects specific fields. |
| `limit` | `List` | `count` | `List` | Truncates the list. |
| `update` | `List` | `store`, `values` | `List` | Updates items in the store. |
| `delete` | `List` | `store` | `List` | Deletes items from the store. |
| `inspect` | - | `store` | `Map` | Returns store metadata (count, config). |
| `list_new` | - | - | `List` | Creates a new empty list. |
| `list_append` | - | `list`, `item` | - | Appends an item to a list. |
| `map_merge` | - | `map1`, `map2` | `Map` | Merges two maps. |
| `call_macro` | - | `name`, `params` | - | Invokes a macro. `params` are injected as variables (scoped). |
| `list_append` | - | `list`, `item` | - | Appends an item to a list. |
| `map_merge` | - | `map1`, `map2` | `Map` | Merges two maps. |

## Advanced Control Flow & Cursor Operations

For complex logic that cannot be expressed as a simple pipeline, the engine supports control flow and low-level cursor manipulation.

### Control Flow Operations

*   **`if`**: Conditional execution.
    *   Args: `condition` (CEL string or bool), `then` (block), `else` (block).
*   **`loop`**: Iterate over a list.
    *   Args: `collection` (var name), `item_var` (name for item), `body` (block).
*   **`call_macro`**: Invoke a stored macro.
    *   Args: `name` (macro name).

### Cursor Operations (B-Tree)

Instead of `scan` (which returns a list), you can manually navigate the B-Tree cursor for precise control.

*   **`first` / `last`**: Position cursor at start/end.
*   **`next` / `previous`**: Move cursor.
*   **`find`**: Position cursor at key. Args: `key`, `desc` (bool).
*   **`get_current_key` / `get_current_value`**: Read data at cursor.
*   **`add`**: Insert new item. Args: `key`, `value`.

**Example: Manual Iteration**
```json
[
  { "op": "open_store", "args": { "name": "employees" }, "result_var": "s" },
  { "op": "first", "args": { "store": "s" } },
  { "op": "loop", "args": {
      "collection": "some_list_to_limit_iterations_or_use_while_if_we_had_it", 
      "body": [
          { "op": "get_current_value", "args": { "store": "s" }, "result_var": "val" },
          { "op": "if", "args": { 
              "condition": "val.age > 50", 
              "then": [ { "op": "call_macro", "args": { "name": "process_senior" } } ] 
          }},
          { "op": "next", "args": { "store": "s" } }
      ]
  }}
]
```

### Example: Inner Join (Nested Loop)

```json
[
  { "op": "open_store", "args": { "name": "orders" }, "result_var": "orders" },
  { "op": "open_store", "args": { "name": "customers" }, "result_var": "customers" },
  { "op": "list_new", "result_var": "results" },
  
  // Scan Orders
  { "op": "scan", "args": { "store": "orders", "limit": 100 }, "result_var": "order_list" },
  
  // Loop Orders and Lookup Customer
  { "op": "loop", "args": {
      "collection": "order_list",
      "item_var": "order",
      "body": [
          // Lookup Customer by ID (order.value.customer_id)
          { "op": "find", "args": { "store": "customers", "key": "order.value.customer_id" }, "result_var": "found" },
          { "op": "if", "args": {
              "condition": "found",
              "then": [
                  { "op": "get_current_value", "args": { "store": "customers" }, "result_var": "cust_val" },
                  // Merge Order and Customer
                  { "op": "map_merge", "args": { "map1": "order.value", "map2": "cust_val" }, "result_var": "joined" },
                  { "op": "list_append", "args": { "list": "results", "item": "joined" } }
              ]
          }}
      ]
  }},
  { "op": "assign", "input_var": "results", "result_var": "output" }
]
```
]
```
