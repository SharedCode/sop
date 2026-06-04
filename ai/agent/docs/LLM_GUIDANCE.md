# LLM Tool Guidance - SOP Agent API

**Reference schemas at:** `docs/schemas/*.json`

## Available Tools

### Bulk Operations (10K+ items)

#### bulk_add
- **Purpose**: Insert multiple items efficiently
- **Schema**: [docs/schemas/bulk_add.json](schemas/bulk_add.json)
- **Required**: `store`, `items`
- **Recommended**: `transaction_mode: "auto_batch"`, `batch_size: 250`
- **Example**:
  ```json
  {
    "store": "users",
    "items": [
      {"key": "user_1", "value": {"name": "John", "email": "john@example.com"}},
      {"key": "user_2", "value": {"name": "Jane", "email": "jane@example.com"}}
    ],
    "transaction_mode": "auto_batch",
    "batch_size": 250
  }
  ```

#### bulk_update
- **Purpose**: Update multiple items efficiently
- **Schema**: Similar to bulk_add
- **Usage**: Same as bulk_add but updates existing items

#### bulk_delete
- **Purpose**: Delete multiple items efficiently  
- **Required**: `store`, `keys` (array of keys to delete)
- **Example**:
  ```json
  {
    "store": "users",
    "keys": ["user_1", "user_2", "user_3"],
    "transaction_mode": "auto_batch"
  }
  ```

### Transactions

#### begin_transaction
- **Schema**: [docs/schemas/transaction.json#TransactionArgs](schemas/transaction.json)
- **Returns**: TransactionHandle with `id` field
- **Example**:
  ```json
  {"database": "dev_db", "mode": "write"}
  ```

#### commit_transaction / rollback_transaction
- **Required**: `transaction_id` (from begin_transaction)

## Transaction Modes

| Mode | Use Case | Items | Behavior |
|------|----------|-------|----------|
| `auto_batch` | Large bulk ops | 10K+ | Tx per batch, auto-commit |
| `single` | Atomic bulk | <10K | ONE tx, single commit |
| `explicit` | Multi-op atomic | Any | Use provided tx, caller commits |

## Decision Tree

```
Need bulk operation?
├─ Yes
│  ├─ Just ONE operation (add/update/delete)?
│  │  └─ Use auto_batch mode (simplest, scales to millions)
│  │
│  └─ Multiple operations must be atomic?
│     └─ Use explicit mode:
│        1. begin_transaction
│        2. bulk_add with transaction_id
│        3. bulk_update with same transaction_id
│        4. commit_transaction
│
└─ No (single items)
   └─ Use add/update/delete/select
```

## Key Benefits

✅ **Concise guidance** - Reference schemas instead of inline definitions  
✅ **Single source** - Schemas generated from Go structs  
✅ **Type-safe** - Validation against JSON schema  
✅ **Accurate** - No drift between docs and implementation  

**150+ lines of schema → 10 lines of guidance + schema reference!**
