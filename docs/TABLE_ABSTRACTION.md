# SOP Schema Format: The Table Abstraction

**Date**: June 4, 2026  
**Status**: Implemented and Active

**Date**: June 4, 2026  
**Status**: Implemented and Active

## Overview

SOP presents its key-value storage layer through an elegant **Table Abstraction**. The Engine removes Key/Value wrapper structures and exposes a flat, SQL-like schema to the AI and application layers. This creates a seamless interface where stores behave like database tables with primary keys and fields.

## The Design Philosophy

### Storage Layer (Internal)
At the storage level, SOP uses a B-tree with Key/Value pairs:
- **Key**: Primary key (simple or composite struct)
- **Value**: Data payload (struct or map)

### Table Abstraction (What AI Sees)
The Engine removes these wrappers and presents a **flat schema**:
- All fields at the same level
- No Key/Value prefixes
- `key_fields` array identifies primary key components
- `value_fields` array identifies data fields

This separation of concerns allows:
- **Storage efficiency** through structured Key/Value pairs
- **Conceptual simplicity** through table-like interfaces
- **SQL familiarity** for queries and predicates

## Schema Format

### Simple Key Example

```json
{
  "name": "users",
  "schema": {
    "key": "string",
    "first_name": "string",
    "age": "number",
    "email": "string"
  },
  "key_fields": ["key"],
  "value_fields": ["first_name", "age", "email"],
  "description": "User profiles",
  "relations": [
    {
      "source_fields": ["key"],
      "target_store": "users_orders",
      "target_fields": ["key"]
    }
  ]
}
```

**Key Points:**
- Schema is flat: `"first_name": "string"`, not `"Value.first_name": "string"`
- `key_fields` explicitly marks which field(s) form the primary key
- Relations reference schema field names directly

### Composite Key Example

```json
{
  "name": "users_by_name",
  "schema": {
    "first_name": "string",
    "last_name": "string",
    "age": "number",
    "email": "string"
  },
  "key_fields": ["first_name", "last_name"],
  "value_fields": ["age", "email"],
  "description": "Users indexed by full name"
}
```

**Key Points:**
- Multiple fields in `key_fields` indicate composite primary key
- All fields remain at the same flat level in schema
- No structural nesting required

## Relations Format

Relations map joins between stores using **schema field names** directly.

### Structure

```go
type Relation struct {
    SourceFields []string `json:"source_fields"`
    TargetStore  string   `json:"target_store"`
    TargetFields []string `json:"target_fields"`
}
```

### Simple Relation Example

```json
{
  "source_fields": ["key"],
  "target_store": "users_orders",
  "target_fields": ["key"]
}
```

This defines: `users.key → users_orders.key`

### Composite Key Relation Example

```json
{
  "source_fields": ["first_name", "last_name"],
  "target_store": "linked_accounts",
  "target_fields": ["owner_first", "owner_last"]
}
```

This defines: `users_by_name.(first_name, last_name) → linked_accounts.(owner_first, owner_last)`

### Multi-Relation Example

From the demo data, the `users` store has multiple relations:

```json
{
  "name": "users",
  "schema": {
    "key": "uuid",
    "age": "number",
    "first_name": "string",
    "last_name": "string",
    "email": "string",
    "gender": "string",
    "country": "string"
  },
  "relations": [
    {
      "source_fields": ["age"],
      "target_store": "users_by_age",
      "target_fields": ["key"]
    },
    {
      "source_fields": ["key"],
      "target_store": "users_orders",
      "target_fields": ["key"]
    }
  ]
}
```

This defines two paths:
1. Index lookup: `users.age → users_by_age.key`
2. Join path: `users.key → users_orders.key`

## Predicate Format

Predicates align with the flat schema format.

### Single-Store Operations

For operations on a single store, use **bare field names**:

```json
{
  "first_name": {"$eq": "John"},
  "age": {"$gt": 30}
}
```

### After Joins

After joining stores, use **store-qualified field names** to disambiguate:

```json
{
  "users.first_name": {"$eq": "John"},
  "orders.total_amount": {"$gt": 500},
  "orders.status": {"$eq": "completed"}
}
```

### Type Alignment

Match schema types exactly:
- **string**: Use quoted values: `{"first_name": {"$eq": "John"}}`
- **number**: Use numeric values: `{"age": {"$gt": 30}}`
- **boolean**: Use boolean values: `{"active": {"$eq": true}}`
- **uuid**: Use quoted UUID strings: `{"key": {"$eq": "550e8400-e29b-41d4-a716-446655440000"}}`

## Complete Example

### Scenario
Find orders for users named "John" where total_amount > 500.

### Step 1: Research Schema

Call `list_stores` with `["users", "users_orders", "orders"]`:

```json
{
  "stores": [
    {
      "name": "users",
      "schema": {
        "key": "uuid",
        "first_name": "string",
        "age": "number"
      },
      "relations": [
        {
          "source_fields": ["key"],
          "target_store": "users_orders",
          "target_fields": ["key"]
        }
      ]
    },
    {
      "name": "users_orders",
      "schema": {
        "key": "uuid",
        "value": "uuid"
      },
      "description": "Link table: UserID -> OrderID",
      "relations": [
        {
          "source_fields": ["value"],
          "target_store": "orders",
          "target_fields": ["key"]
        }
      ]
    },
    {
      "name": "orders",
      "schema": {
        "key": "uuid",
        "total_amount": "number",
        "status": "string"
      }
    }
  ]
}
```

### Step 2: Build Query Script

```json
{
  "script": [
    {
      "op": "begin_tx",
      "args": {"mode": "read"},
      "result_var": "tx"
    },
    {
      "op": "open_store",
      "args": {"transaction": "tx", "name": "users"},
      "result_var": "users_store"
    },
    {
      "op": "select",
      "args": {
        "store": "users_store",
        "condition": {"first_name": {"$eq": "John"}}
      },
      "result_var": "matched_users"
    },
    {
      "op": "join",
      "input_var": "matched_users",
      "args": {
        "target": "users_orders_store",
        "relation": "users_orders"
      },
      "result_var": "user_order_links"
    },
    {
      "op": "join",
      "input_var": "user_order_links",
      "args": {
        "target": "orders_store",
        "relation": "orders"
      },
      "result_var": "joined_orders"
    },
    {
      "op": "filter",
      "input_var": "joined_orders",
      "args": {
        "condition": {"orders.total_amount": {"$gt": 500}}
      },
      "result_var": "filtered_orders"
    },
    {
      "op": "return",
      "input_var": "filtered_orders"
    }
  ]
}
```

### Key Observations

1. **Single-store predicate**: `{"first_name": {"$eq": "John"}}` uses bare field name
2. **Post-join predicate**: `{"orders.total_amount": {"$gt": 500}}` uses store-qualified name
3. **Relations**: Referenced by name (`"users_orders"`, `"orders"`) from schema
4. **Type matching**: String "John" quoted, number 500 unquoted

## Why This is Elegant

### 1. Separation of Concerns
- **Storage layer**: Optimized key-value structure for performance
- **Application layer**: Clean table abstraction for simplicity

### 2. Consistency
- Schema, relations, and predicates all use the same flat field names
- No mental translation between storage format and query format

### 3. SQL Familiarity
- Looks like SQL tables with primary keys
- Joins use familiar source→target field mappings
- Predicates feel like SQL WHERE clauses

### 4. Composite Key Support
- Naturally handles multi-field primary keys
- Relations can map multiple fields without special syntax
- Schema remains flat regardless of key complexity

### 5. Type Safety
- Schema declares types explicitly
- Predicates must align with schema types
- AI can validate queries before execution

## Implementation

The Engine implements this abstraction through:

1. **Schema Extraction** (`ai/agent/copilottools.go`):
   - Flattens Key struct fields into schema
   - Flattens Value struct fields into schema
   - Populates `key_fields` and `value_fields` arrays

2. **Predicate Execution** (`ai/agent/engine_native.go`):
   - Interprets flat field names in predicates
   - Maps to internal Key/Value structure for storage operations
   - Handles store-qualified names after joins

3. **AI Instructions** (`ai/agent/copilottools.go`):
   - Documents flat format in tool instructions
   - Provides worked examples using schema field names
   - Guides AI to align predicates with schema types

## Testing

Comprehensive test coverage validates the abstraction:

- `TestToolListStores`: Verifies flat schema format
- `TestToolListStores_StructKeySchema`: Tests composite key flattening  
- `TestExecuteScript_RelationJoin`: Validates predicates align with schema
- Tests confirm no `Key.` or `Value.` prefixes in schema or predicates

## Future: Typed Go API

The `/memories/repo/typed-api-refactoring.md` document outlines plans to extend this table abstraction to the typed Go API, removing Key/Value wrappers from application code while maintaining storage efficiency.
