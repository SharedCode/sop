# Agent API Architecture

## Business Context: Stores vs Spaces

### SOP's Unique Dual Capability

**SOP is the only platform that provides BOTH transactional B-Tree stores AND AI-powered knowledge spaces under a unified ACID transaction layer.**

Most database vendors force you to choose:
- **Traditional Databases** (PostgreSQL, MongoDB, Redis): Excellent for structured data, but no native AI/vector capabilities
- **Vector Databases** (Pinecone, Weaviate, Chroma): Great for semantic search, but weak transactional guarantees, poor structured data support, and **K-means clustering that makes visualization architecturally impossible**

**SOP delivers both in one platform**, with revolutionary architecture:

1. **Stores**: Battle-tested B-Tree storage for structured business data
2. **Spaces**: **Relativity-based vector architecture** (not K-means) for AI knowledge
3. **Visual Management**: The only vector database with full visual editing (KnowledgeBase Studio)
4. **Unified Transactions**: Join Stores and Spaces in the same ACID transaction
5. **Mint → Refine → Vectorize**: Build structure first, add content, vectorize when ready

### Why SOP Spaces Are Revolutionary

**Traditional Vector Databases** use K-means clustering:
- ❌ Pigeon hole problem (forced clustering)
- ❌ No human-readable structure (statistical centroids)
- ❌ Requires periodic optimization (rebalancing)
- ❌ **Visualization is architecturally impossible**

**SOP Spaces** use relativity-based architecture:
- ✅ Solved pigeon hole problem
- ✅ Categories and Items are first-class entities
- ✅ Self-balancing, no optimization needed
- ✅ **Full visual management through KnowledgeBase Studio**

See [STORES_VS_SPACES.md](STORES_VS_SPACES.md) for detailed architectural comparison.

### When to Use Stores (Database API)

**Stores** are traditional B-Tree key-value storage for structured data.

#### Use Cases:
- ✅ **Transactional Business Data**: Orders, users, inventory, transactions
- ✅ **Bulk Operations**: Loading 10K+ records with batch processing
- ✅ **Relational Queries**: Joins across multiple stores
- ✅ **High-Volume CRUD**: Millions of structured records
- ✅ **Complex Keys**: Composite keys (Region + Dept + ID) for efficient range queries
- ✅ **Test Harnesses**: Direct programmatic control for performance testing

#### Business Value:
- **ACID Transactions**: Full consistency guarantees
- **Proven Technology**: B-Trees are battle-tested for 50+ years
- **Predictable Performance**: O(log N) lookups, range scans
- **Language Agnostic**: OpenAPI clients for Go, Python, Rust, C#, Java
- **Unlimited Scale**: Terabytes of data with erasure coding & replication

#### Example Scenarios:
```
"Store 100,000 customer orders with atomic commit"
"Bulk insert 5M log records in batches of 1000"
"Join users and orders on user_id"
"Update inventory across 50 warehouses transactionally"
```

### When to Use Spaces (Space API)

**Spaces** are AI-powered knowledge bases with semantic search, embeddings, and categorization.

#### Use Cases:
- ✅ **AI Memory & RAG**: Store LLM-generated knowledge for retrieval
- ✅ **Semantic Search**: Find conceptually similar content, not just keywords
- ✅ **Knowledge Organization**: Categories, items, hierarchical structure
- ✅ **Embeddings & Vectors**: Store and search high-dimensional vectors
- ✅ **Dynamic Content**: LLM outputs, summaries, generated insights
- ✅ **Domain-Specific Knowledge**: Medical, legal, technical documentation

#### Business Value:
- **Semantic Understanding**: Vector similarity search powered by embeddings
- **Organized Knowledge**: Category-based structure (not flat key-value)
- **AI-Native**: Designed for LLM workflows (RAG, chat, Q&A)
- **Text Search**: BM25 full-text search alongside vector search
- **Transactional**: Unlike most vector DBs, Spaces support ACID transactions
- **Isolated Domains**: Each Space is independent (medical, HR, sales)

#### Example Scenarios:
```
"Create a Medical Knowledge Space with research papers"
"Store LLM-generated summaries in Categories by topic"
"Search for documents semantically similar to 'heart disease treatment'"
"Vectorize 10K documents with automatic embedding generation"
"Enrich Space with AI-generated metadata and relationships"
```

### The Power of Both

**Unique SOP Advantage**: Use Stores and Spaces together in the same application.

#### Hybrid Workflows:
1. **Store structured data in Stores** (customer records, orders, products)
2. **Store AI knowledge in Spaces** (product descriptions, reviews, recommendations)
3. **Join them in queries** to power AI applications with business data

#### Example: E-commerce with AI
```go
// 1. Traditional data in Stores
orders := agent.Select(ctx, SelectArgs{Store: "orders", Filter: "user_id=123"})

// 2. AI knowledge in Spaces
recommendations := space.SearchItems(ctx, SpaceSearchArgs{
    KBName: "products",
    Query: "wireless headphones with noise cancellation",
    Limit: 10,
})

// 3. Both in the same transaction!
tx := agent.BeginTransaction(ctx, TransactionArgs{Mode: "write"})
agent.Add(ctx, AddArgs{TransactionID: tx.ID, ...})
space.UpsertItem(ctx, UpsertItemArgs{TransactionID: tx.ID, ...})
agent.CommitTransaction(ctx, TransactionCommitArgs{TransactionID: tx.ID})
```

### Decision Matrix

| Requirement | Use Stores | Use Spaces |
|-------------|-----------|------------|
| Structured business data | ✅ | ❌ |
| Semantic/vector search | ❌ | ✅ |
| Bulk inserts (10K+ items) | ✅ | ⚠️ (use bulk ops) |
| Relational joins | ✅ | ❌ |
| AI-generated content | ⚠️ | ✅ |
| Category organization | ❌ | ✅ |
| OpenAPI/multi-language | ✅ | 🔄 (coming) |
| ACID transactions | ✅ | ✅ |

**When in doubt**: If you're building AI features (RAG, chat, semantic search), use Spaces. For everything else, use Stores.

### Visual Management

**SOP Data Manager** provides complete visual management for both Stores and Spaces through an intuitive web interface:

#### Store Management
- **Full CRUD Operations** - Add, edit, delete, view records with JSON editor
- **Rich Search** - Key prefix search, complex multi-field queries
- **High-Performance Navigation** - Pagination for millions of records
- **Schema Designer** - Visual editor for Index Specifications and CEL expressions
- **Store Inspector** - View metadata, sample keys, counts

#### KnowledgeBase Studio (Spaces)
- **Category Management** - Create, edit, delete categories hierarchically
- **Item Operations** - Add, update, delete items with rich metadata
- **Export/Import** - Backup and restore entire Spaces (JSON/JSONL)
- **Vectorization Controls** - Full-space, category, or item-level vectorization
- **Configuration Editor** - Visual editor for Space settings
- **Semantic Search UI** - Search interface for testing queries

**Revolutionary Architecture**: SOP is the **only vector database with visual management**—not because of better UI engineering, but because of **fundamentally different architecture**:

- **K-means Vector DBs** (Pinecone, Weaviate, Chroma): Use clustering with statistical centroids. Visualization is **architecturally impossible** because centroids are mathematical abstractions, not human-understandable entities.

- **SOP's Spaces** (Relativity-Based): Use Theory of Relativity principles for vector organization. Categories and Items are **first-class entities**, making visualization **inherent** to the design.

**The Lifecycle**: Traditional vector DBs force "Insert → Query". SOP enables **"Mint → Refine → Vectorize"**—build structure first, add content, then vectorize when ready. All visually manageable in KnowledgeBase Studio.

**Competitive Edge**: Most platforms provide only API access or basic dashboards. SOP provides a complete visual management suite with AI Copilot integration for natural language queries and code generation.

See [STORES_VS_SPACES.md](STORES_VS_SPACES.md) for detailed capabilities and architectural comparison.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Business Logic Layer                     │
│                                                              │
│  ┌──────────────────────────┐  ┌─────────────────────────┐ │
│  │  Spaces API              │  │  Database API           │ │
│  │  (AI Memory)             │  │  (Data Operations)      │ │
│  │                          │  │                         │ │
│  │  585 lines, 14 functions │  │  4 files, typed structs │ │
│  │  map[string]any          │  │  OpenAPI generation     │ │
│  └───────────┬──────────────┘  └──────────┬──────────────┘ │
└──────────────┼─────────────────────────────┼────────────────┘
               │                             │
       ┌───────┴────────┐            ┌──────┴──────┬──────────┐
       │                │            │             │          │
  ┌────▼────┐   ┌──────▼────┐  ┌───▼────┐  ┌─────▼────┐  ┌──▼──────┐
  │ LLM     │   │ HTTP/     │  │ Test   │  │ Scripts  │  │ Direct  │
  │ Tools   │   │ OpenAPI   │  │ Harness│  │ Playback │  │ Go Code │
  └─────────┘   └───────────┘  └────────┘  └──────────┘  └─────────┘
```

## 1. Spaces API (Domain: AI Memory)

**File**: `copilottools.space.go` (585 lines)  
**Purpose**: High-level AI memory operations

### Tools (8 Registered)
- `mint_to_space` - Store generated knowledge in Space
- `delete_space` - Delete entire Space
- `enrich_space` - Refresh derived knowledge
- `update_space_config` / `read_space_config` - Configuration management
- `vectorize_space` - Full Space vectorization
- `vectorize_space_categories` - Category-scoped vectorization
- `vectorize_space_items` - Item-scoped vectorization

### Internal Operations (6 Additional)
- Category CRUD: upsert, delete, list
- Item CRUD: upsert, delete, list, search_by_path

### Architecture
- **Type System**: `map[string]any` with inline JSON schemas
- **Domain**: Knowledge Bases, VectorDB, Embeddings
- **Schema**: Special structure (Categories/Items)
- **Usage**: AI memory subsystem operations

### Key Rule
**When working with Spaces, DO NOT USE RAW DATABASE TOOLS**

Spaces are AI memory (semantic search, embeddings, categories), not raw B-Trees. Using database tools on Spaces causes schema validation errors.

## 2. Database API (Domain: Data Operations)

**Files**: `api_types.go`, `api_core.go`, `api_bulk.go`, `api_transaction.go`  
**Purpose**: Low-level database operations with bulk scalability

### Operations

#### Single Operations (api_core.go)
```go
Add(ctx, AddArgs) (string, error)
Update(ctx, UpdateArgs) (string, error)
Delete(ctx, DeleteArgs) (string, error)
Select(ctx, SelectArgs) (string, error)
ExecuteScript(ctx, ExecuteScriptArgs) (string, error)
Join(ctx, JoinArgs) (string, error)
```

#### Bulk Operations (api_bulk.go)
```go
BulkAdd(ctx, BulkAddArgs) (*BulkOperationResult, error)
BulkUpdate(ctx, BulkUpdateArgs) (*BulkOperationResult, error)
BulkDelete(ctx, BulkDeleteArgs) (*BulkOperationResult, error)
```

**Transaction Modes**:
- `auto_batch` (default) - Scalable for 10K+ items (per-batch tx)
- `single` - Atomic for <10K items (one tx)
- `explicit` - Multi-operation atomicity (provided tx handle)

#### Transaction Management (api_transaction.go)
```go
BeginTransaction(ctx, TransactionArgs) (*TransactionHandle, error)
CommitTransaction(ctx, TransactionCommitArgs) error
RollbackTransaction(ctx, TransactionRollbackArgs) error
```

### Architecture
- **Type System**: Strongly typed Go structs with JSON tags
- **Schema Generation**: OpenAPI 2.0 via swag annotations
- **Domain**: B-Trees, raw database operations
- **Usage**: Bulk operations, test harnesses, programmatic access

### OpenAPI Generation

```bash
cd ai/agent
make openapi  # Generates docs/agent_swagger.yaml
```

**Benefits**:
- ✅ **93% reduction** in LLM guidance size (10 lines vs 150+)
- ✅ **Single source of truth** - Schemas from Go structs
- ✅ **Always accurate** - Can't drift (auto-generated)
- ✅ **Type-safe** - Compile-time validation
- ✅ **Multi-language** - Generate clients for any language

## When to Use Which API

### Use Spaces API When:
- ✅ Creating/managing knowledge bases
- ✅ Storing AI-generated content
- ✅ Semantic search operations
- ✅ Vectorization/embeddings
- ✅ Category/Item organization
- ✅ User says: "Create Notes space", "Enrich my knowledge base"

### Use Database API When:
- ✅ Bulk CRUD operations (10K+ items)
- ✅ Transaction control needed
- ✅ Direct B-Tree access
- ✅ Test harness development
- ✅ Performance benchmarking
- ✅ User says: "Bulk insert 50,000 records", "Run atomic updates"

## LLM Integration Pattern

Both APIs expose tools via thin adapters:

```go
// LLM tool adapter (map[string]any)
func (a *CopilotAgent) toolBulkAdd(ctx, args map[string]any) (string, error) {
    var typed BulkAddArgs
    mapToStruct(args, &typed)  // One-line conversion
    return a.BulkAdd(ctx, typed)  // Call typed API
}

// Typed API (structs)
func (a *CopilotAgent) BulkAdd(ctx, args BulkAddArgs) (*BulkOperationResult, error) {
    // Implementation with type safety
}
```

**LLMs continue using JSON/text** - No changes to LLM interaction  
**Developers gain type safety** - Direct struct-based API access  
**HTTP endpoints validated** - OpenAPI schema enforcement  

## Transaction Mode Decision Tree

```
Need bulk operation?
├─ Yes
│  ├─ Just ONE operation (add/update/delete)?
│  │  └─ Use auto_batch mode
│  │     - Simplest, scales to millions
│  │     - Per-batch commits
│  │
│  └─ Multiple operations must be atomic?
│     └─ Use explicit mode:
│        1. BeginTransaction
│        2. BulkAdd with transaction_id
│        3. BulkUpdate with same transaction_id
│        4. CommitTransaction
│        - All-or-nothing across operations
│
└─ No (single items or small datasets)
   └─ Use single operations (Add/Update/Delete)
```

## Example: Bulk Operations

### Natural Language (LLM)
```
User: "Bulk insert 50,000 user records with auto-batching"

Agent calls: bulk_add
Args (JSON): {
  "store": "users",
  "items": [...],
  "transaction_mode": "auto_batch",
  "batch_size": 250
}
```

### Typed API (Go)
```go
result, err := agent.BulkAdd(ctx, BulkAddArgs{
    Store: "users",
    Items: items,  // 50,000 items
    TransactionMode: TransactionModeAutoBatch,
    BatchSize: 250,
})

fmt.Printf("Processed: %d in %s (%.0f items/sec)\n",
    result.Processed, 
    result.Duration,
    result.Metrics.ItemsPerSecond)
```

### HTTP/OpenAPI
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

## Documentation

- **User Guide**: `/ai/AI_COPILOT_USAGE.md` - Section 2.1 (Database API), Section 3 (Spaces API)
- **Architecture**: `/ai/AI_SCRIPT_ARCHITECTURE.md` - API Architecture section
- **API Reference**: `/ai/agent/README_API.md` - Complete Database API docs
- **Implementation**: `/IMPLEMENTATION.md` - Typed Database API implementation status
- **Schemas**: `/ai/agent/docs/schemas/*.json` - JSON schemas for LLM reference
- **OpenAPI**: `/ai/agent/docs/agent_swagger.yaml` - Generated OpenAPI 2.0 spec

## Future: Spaces API Migration

The Spaces API currently uses inline JSON schemas. It can benefit from the same OpenAPI generation pattern:

```go
// Future: Spaces API with typed structs + OpenAPI
type MintToSpaceArgs struct {
    KBName   string `json:"kb_name" binding:"required"`
    Content  string `json:"content" binding:"required"`
    Category string `json:"category,omitempty"`
}

// @Summary Mint content to Space
// @Param args body MintToSpaceArgs true "Mint parameters"
func (a *CopilotAgent) MintToSpace(ctx, args MintToSpaceArgs) (string, error)
```

**Benefits**: Same schema accuracy improvement, multi-language clients, HTTP validation.

## Key Takeaways

1. **Two APIs, Two Domains**: Spaces (AI memory) vs Database (data operations)
2. **Complementary, Not Competing**: Both coexist for different use cases
3. **OpenAPI = Accuracy**: Single source of truth, no schema drift
4. **LLM Compatibility**: Dual-layer design preserves text/JSON interaction
5. **Type Safety Bonus**: Developers gain compile-time validation
6. **Scalability Focus**: Bulk operations handle millions of items
7. **Transaction Flexibility**: Three modes balance atomicity vs scalability
