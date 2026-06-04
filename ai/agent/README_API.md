# SOP Agent API

Strongly typed API for AI-powered database operations with OpenAPI support.

## Business Context: Stores vs Spaces

SOP uniquely provides **two complementary data systems**:

### 1. **Stores** (Database API) - Structured Data
Traditional B-Tree key-value storage for business data:
- **What**: Orders, users, inventory, transactions, logs
- **Why**: ACID transactions, proven performance, relational queries
- **Scale**: Millions of records, bulk operations, joins
- **Languages**: OpenAPI clients for Go, Python, Rust, C#, Java

**Example**: `BulkAdd(10K orders) → Select(filter by date) → Join(users & orders)`

### 2. **Spaces** (Space API) - AI Knowledge
Semantic knowledge bases with vector search:
- **What**: LLM outputs, documents, embeddings, summaries
- **Why**: Semantic search, category organization, RAG pipelines
- **Scale**: Organized by categories, vector similarity search
- **AI-Native**: Designed for LLM workflows

**Example**: `CreateSpace("Medical") → UpsertItems(research papers) → SearchSimilar("heart disease")`

### Why SOP is Unique

**Most vendors make you choose**:
- PostgreSQL/MongoDB: Great structured data, no AI
- Pinecone/Weaviate: Great vectors, weak transactions, **K-means clustering (no visualization)**

**SOP delivers both + revolutionary architecture**:
- Store business data in Stores (B-Tree)
- Store AI knowledge in Spaces (**Relativity-based**, not K-means)
- Use them together in the same transaction
- **Visual management** that's architecturally impossible for K-means vector DBs

### Revolutionary Vector Architecture

**Why other vector databases can't offer visualization**:
- Use K-means clustering with statistical centroids
- Pigeon hole problem (forced clustering)
- Requires periodic optimization (rebalancing)
- Structure is mathematically abstract, not human-understandable

**SOP's Spaces solved this**:
- Theory of Relativity principles (not K-means)
- Categories and Items are first-class entities
- Self-balancing, no optimization needed
- **Mint → Refine → Vectorize** lifecycle

### Visual Management: SOP Data Manager

SOP includes a complete visual management suite with:
- **Store Management**: Full CRUD, JSON editor, schema designer, bulk operations
- **KnowledgeBase Studio**: Category builder, item manager, vectorization controls, export/import
- **AI Copilot**: Natural language queries and code generation
- **All in one interface**: Environment switching, role-based access

**KnowledgeBase Studio** is the world's only visual vector database management tool—enabled by SOP's relativity-based architecture.

See [API_ARCHITECTURE.md](API_ARCHITECTURE.md) and [STORES_VS_SPACES.md](STORES_VS_SPACES.md) for detailed architectural comparison.

---

## Quick Start

### Generate OpenAPI Spec

```bash
cd ai/agent
make openapi-install  # First time only
make openapi          # Generates docs/swagger.yaml
```

### View Documentation

```bash
make openapi-serve    # Opens at http://localhost:8080
```

Or manually:
```bash
# View YAML
cat docs/swagger.yaml

# Use with LLMs
curl http://localhost:8080/api/v1/docs/swagger.yaml
```

## API Design

### Dual-Layer Architecture

```
┌─────────────────────────────────────┐
│     Business Logic (Typed API)      │
│   BulkAdd(ctx, BulkAddArgs)        │
└───────────────┬─────────────────────┘
                │
        ┌───────┴────────┬──────────────┐
        │                │              │
  ┌─────▼──────┐  ┌─────▼─────┐  ┌────▼──────┐
  │ LLM Tools  │  │ HTTP API  │  │ Test Code │
  │ (map[any]) │  │ (OpenAPI) │  │ (structs) │
  └────────────┘  └───────────┘  └───────────┘
```

### LLM Integration

LLMs continue using existing text/JSON tools:
- Tool registry provides map[string]any interface
- Thin adapters convert to typed API
- No changes to LLM interaction

### HTTP Integration

OpenAPI spec enables:
- **Auto-generated clients** (Python, TypeScript, Rust, etc.)
- **API documentation** (Swagger UI)
- **Schema validation** (automatic request validation)
- **LLM function calling** (structured schemas)

## Transaction Modes

### auto_batch (Default)
```go
BulkAdd(ctx, BulkAddArgs{
    Items: items,  // 10K+ items
    TransactionMode: TransactionModeAutoBatch,
    BatchSize: 250,
})
// Creates tx per batch, auto-commits
// Best for: Large bulk inserts
```

### single
```go
BulkAdd(ctx, BulkAddArgs{
    Items: items,  // <10K items
    TransactionMode: TransactionModeSingle,
})
// ONE transaction for ALL items
// Best for: Atomic bulk operations
```

### explicit
```go
tx, _ := BeginTransaction(ctx, TransactionArgs{Mode: "write"})

BulkAdd(ctx, BulkAddArgs{
    Items: batch1,
    TransactionID: tx.ID,
    TransactionMode: TransactionModeExplicit,
})

BulkUpdate(ctx, BulkUpdateArgs{
    Items: batch2,
    TransactionID: tx.ID,
    TransactionMode: TransactionModeExplicit,
})

CommitTransaction(ctx, TransactionCommitArgs{TransactionID: tx.ID})
// Best for: Multi-operation atomicity
```

## OpenAPI Benefits

### Before (Verbose Schema in Code)

```go
registry.Register("bulk_add", "Bulk insert", map[string]any{
    "type": "object",
    "properties": map[string]any{
        "store": map[string]any{"type": "string", "description": "..."},
        "items": map[string]any{
            "type": "array",
            "items": map[string]any{...}, // 100+ lines
        },
        "transaction_mode": map[string]any{
            "enum": []string{"auto_batch", "single", "explicit"},
            "description": "Transaction mode: auto_batch for...",
        },
    },
}, toolBulkAdd)
```

### After (Reference Schema)

```go
// LLM guidance (10 lines vs 100+)
// Available at: http://api.sop.dev/openapi.yaml
// 
// bulk_add: See BulkAddArgs schema
// transaction_mode: See TransactionMode enum
//
// For 10K+ items: transaction_mode = "auto_batch"
```

**OpenAPI spec becomes single source of truth!**

## Example Usage

### Direct API (Test Harness)

```go
func TestBulkPerformance(t *testing.T) {
    agent := NewCopilotAgent()
    
    result, err := agent.BulkAdd(ctx, BulkAddArgs{
        Store: "users",
        Items: generateTestData(10000),
        TransactionMode: TransactionModeAutoBatch,
        BatchSize: 250,
    })
    
    assert.NoError(t, err)
    assert.Equal(t, 10000, result.Processed)
    assert.Less(t, result.Duration, 5*time.Second)
}
```

### LLM Tool (No Change)

```go
// LLM generates JSON
{
    "tool": "bulk_add",
    "args": {
        "store": "users",
        "items": [...],
        "transaction_mode": "auto_batch"
    }
}

// Adapter converts and calls typed API
toolBulkAdd(ctx, args) → BulkAdd(ctx, BulkAddArgs{...})
```

### HTTP Endpoint

```bash
curl -X POST http://localhost:8080/api/v1/bulk-add \
  -H "Content-Type: application/json" \
  -d '{
    "store": "users",
    "items": [...],
    "transaction_mode": "auto_batch",
    "batch_size": 250
  }'
```

## Files

- `api_types.go` - Type definitions with JSON tags
- `api_core.go` - Single-operation methods
- `api_bulk.go` - Bulk operations with transaction control
- `api_transaction.go` - Transaction lifecycle
- `api_openapi.go` - OpenAPI annotations
- `docs/swagger.yaml` - Generated OpenAPI spec (reference this!)

## Generating Specs

The OpenAPI spec is generated from:
1. **Go struct tags** (`json:"field"`, `example:"value"`)
2. **godoc comments** (method descriptions)
3. **Annotations** (`@Summary`, `@Param`, `@Success`)

Update the spec:
```bash
make openapi  # Regenerate after code changes
```

## Benefits Summary

✅ **Single source of truth** - Schemas derived from Go structs  
✅ **Less verbose** - Reference schemas instead of inline definitions  
✅ **Auto-validated** - HTTP frameworks validate against spec  
✅ **Multi-language** - Generate clients for any language  
✅ **LLM-compatible** - Function calling with structured schemas  
✅ **Type-safe** - Compile-time checks for Go code  
✅ **Test-friendly** - Use typed API directly in tests  

**This is the missing piece for LLM accuracy!** 🎯
