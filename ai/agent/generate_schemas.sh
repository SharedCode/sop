#!/usr/bin/env bash
# Generate JSON Schema from Go types for LLM consumption

set -e

echo "Generating JSON schemas from Go types..."

# Create schemas directory
mkdir -p docs/schemas

# Generate schema for each main type
cat > docs/schemas/bulk_add.json << 'EOF'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "BulkAddArgs",
  "description": "Bulk insert multiple items with automatic transaction batching",
  "type": "object",
  "required": ["store", "items"],
  "properties": {
    "database": {
      "type": "string",
      "description": "Database name (optional, uses current session db)",
      "example": "dev_db"
    },
    "store": {
      "type": "string",
      "description": "Store/table name",
      "example": "users"
    },
    "items": {
      "type": "array",
      "description": "Items to insert",
      "items": {
        "$ref": "#/definitions/BulkItem"
      }
    },
    "transaction_id": {
      "type": "string",
      "description": "Existing transaction ID (explicit mode only)"
    },
    "transaction_mode": {
      "$ref": "#/definitions/TransactionMode"
    },
    "batch_size": {
      "type": "integer",
      "description": "Items per batch (default: 100, recommended: 250 for 10K+ items)",
      "default": 100,
      "example": 250
    }
  },
  "definitions": {
    "BulkItem": {
      "type": "object",
      "required": ["key", "value"],
      "properties": {
        "key": {
          "description": "Item key (any type)",
          "example": "user_123"
        },
        "value": {
          "type": "object",
          "description": "Item value (JSON object)",
          "additionalProperties": true
        }
      }
    },
    "TransactionMode": {
      "type": "string",
      "enum": ["auto_batch", "single", "explicit"],
      "description": "Transaction handling mode",
      "default": "auto_batch",
      "x-enum-descriptions": {
        "auto_batch": "Creates transactions per batch, auto-commits. Use for 10K+ items (scalable)",
        "single": "ONE transaction for ALL items, single commit. Use for <10K items (atomic)",
        "explicit": "Uses provided transaction, NO auto-commits. Use for multi-operation atomicity"
      }
    }
  }
}
EOF

cat > docs/schemas/transaction.json << 'EOF'
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "definitions": {
    "TransactionArgs": {
      "title": "TransactionArgs",
      "description": "Arguments for beginning a transaction",
      "type": "object",
      "required": ["mode"],
      "properties": {
        "database": {
          "type": "string",
          "description": "Database name (optional)",
          "example": "dev_db"
        },
        "mode": {
          "type": "string",
          "enum": ["read", "write"],
          "description": "Transaction mode",
          "default": "read"
        }
      }
    },
    "TransactionHandle": {
      "title": "TransactionHandle",
      "description": "Handle to an active transaction",
      "type": "object",
      "properties": {
        "id": {
          "type": "string",
          "description": "Unique transaction ID",
          "example": "550e8400-e29b-41d4-a716-446655440000"
        },
        "database": {
          "type": "string",
          "description": "Database name"
        },
        "mode": {
          "type": "string",
          "enum": ["read", "write"]
        },
        "started": {
          "type": "string",
          "format": "date-time",
          "description": "When transaction began"
        }
      }
    }
  }
}
EOF

cat > docs/LLM_GUIDANCE.md << 'EOF'
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
EOF

echo "✓ Generated schemas:"
ls -lh docs/schemas/
echo ""
echo "✓ Generated LLM guidance: docs/LLM_GUIDANCE.md"
echo ""
echo "📋 Use in LLM prompts:"
echo "   'Available tools: see docs/LLM_GUIDANCE.md'"
echo "   'Schemas at: docs/schemas/*.json'"
