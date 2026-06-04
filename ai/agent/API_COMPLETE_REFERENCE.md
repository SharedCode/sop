# Complete SOP API Reference: Connecting All the Dots

## Business Context: Why Two APIs?

**SOP's Unique Value Proposition**: The only platform offering both **Stores** (structured data) and **Spaces** (AI knowledge) under unified ACID transactions—with revolutionary vector architecture enabling full visual management.

### Revolutionary Architecture

**Traditional Vector Databases** (Pinecone, Weaviate, Chroma):
- Use K-means clustering → pigeon hole problem
- Statistical centroids → no human-readable structure
- **Visualization is architecturally impossible**

**SOP's Spaces**:
- Relativity-based architecture → solved pigeon hole
- Categories/Items as first-class entities → human-readable
- **Full visual management** through KnowledgeBase Studio

See [STORES_VS_SPACES.md](STORES_VS_SPACES.md) for detailed architectural comparison.

### Quick Decision Guide

**Use Stores (Database API) when:**
- Storing structured business data (orders, users, inventory)
- Need bulk operations (10K+ records)
- Performing relational joins
- Building test harnesses or analytics

**Use Spaces (Space API) when:**
- Storing AI-generated content (summaries, embeddings)
- Need semantic/vector search
- Organizing knowledge by categories
- Building RAG (Retrieval-Augmented Generation) applications

**Use Both Together** to power AI applications with business data in the same transaction.

See [API_ARCHITECTURE.md](API_ARCHITECTURE.md#business-context-stores-vs-spaces) for detailed guidance.

---

## Executive Summary

This document provides a **complete inventory** of all SOP APIs across 7 layers, preventing duplication and ensuring proper integration. Each API is categorized by layer, implementation status, and integration points.

---

## API Layer Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer 7: HTTP REST API (Public Endpoints for End Users)        │
│  Status: ✅ Deployed | Integration: ⚠️ Needs Typed API Connection│
└─────────────────────────────────┬────────────────────────────────┘
                                  │
┌─────────────────────────────────┴────────────────────────────────┐
│  Layer 6: Ask Pipeline (AI Chat & Agent Orchestration)          │
│  Status: ✅ Complete | Integration: ✅ Connected to Agents       │
└─────────────────────────────────┬────────────────────────────────┘
                                  │
┌─────────────────────────────────┴────────────────────────────────┐
│  Layer 5: Script API (Automation & Workflows)                    │
│  Status: ✅ Complete | Integration: ✅ Has TransactionMode       │
└─────────────────────────────────┬────────────────────────────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ Layer 4a:       │    │ Layer 4b:        │    │ Layer 4c:        │
│ Typed Database  │    │ Typed Space API  │    │ JSON Store API   │
│ API (NEW)       │    │ (NEW)            │    │ (PUBLIC)         │
│ Status: ✅ Done │    │ Status: ⚠️ 90%   │    │ Status: ✅ Done  │
│ Integration: ⚠️ │    │ Integration: ⚠️  │    │ Integration: ✅   │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬────────┘
          │                      │                        │
          └──────────────────────┼────────────────────────┘
                                 │
                                 ▼
                ┌────────────────────────────────┐
                │ Layer 3: AI Database API       │
                │ Status: ✅ Complete             │
                │ Integration: ✅ Connected       │
                └────────────────┬───────────────┘
                                 │
                                 ▼
                ┌────────────────────────────────┐
                │ Layer 2: Space Storage         │
                │ (KnowledgeBase CRUD)           │
                │ Status: ✅ Complete             │
                └────────────────┬───────────────┘
                                 │
                                 ▼
                ┌────────────────────────────────┐
                │ Layer 1: Core B-Tree API       │
                │ (Foundation - Transactional)   │
                │ Status: ✅ Complete             │
                │ Bindings: ✅ Go, Rust, C#,     │
                │           Java, Python         │
                └────────────────────────────────┘
```

---

## Layer 7: HTTP REST API (Public Endpoints)

### 7.1 Store Operations (Current - Direct DB Access)

| Endpoint | Method | Purpose | Current Implementation | Typed API Target |
|----------|--------|---------|------------------------|------------------|
| `/api/stores` | GET | List all stores | ✅ `db.GetStores()` | N/A (read-only) |
| `/api/store/add` | POST | Create new store | ✅ Direct B-Tree | `agent.CreateStore()` 🎯 |
| `/api/store/delete` | POST | Delete store | ✅ `database.RemoveBtree()` | `agent.DeleteStore()` 🎯 |
| `/api/store/info` | GET | Get store metadata | ✅ `t.StoreRepository.Get()` | N/A (read-only) |
| `/api/store/update` | POST | Update store config | ✅ `t.StoreRepository.Update()` | `agent.UpdateStoreInfo()` 🎯 |
| `/api/store/items` | GET | List items in store | ✅ `store.First/Next()` | N/A (read-only) |
| `/api/store/item/add` | POST | Add single item | ✅ `store.Add()` | ✅ `agent.Add()` |
| `/api/store/item/update` | POST | Update single item | ✅ `store.Update()` | ✅ `agent.Update()` |
| `/api/store/item/delete` | POST | Delete single item | ✅ `store.Remove()` | ✅ `agent.Delete()` |

**MISSING Endpoints (Need to Add)**:
- ❌ `/api/store/items/bulk` → `agent.BulkAdd()`
- ❌ `/api/store/items/bulk/update` → `agent.BulkUpdate()`
- ❌ `/api/store/items/bulk/delete` → `agent.BulkDelete()`

### 7.2 Space Operations (Current - Direct aidb.Database Access)

| Endpoint | Method | Purpose | Current Implementation | Typed API Target |
|----------|--------|---------|------------------------|------------------|
| `/api/spaces` | GET | List all spaces | ✅ `db.GetDomains()` | N/A (read-only) |
| `/api/spaces/create` | POST | Create/open space | ✅ `db.OpenKnowledgeBase()` | ✅ `agent.CreateSpace()` |
| `/api/spaces/delete` | POST | Delete space | ✅ `db.RemoveKnowledgeBase()` | ✅ `agent.DeleteSpace()` |
| `/api/spaces/config/get` | GET | Get space config | ✅ `kb.GetConfig()` | ✅ `agent.ReadSpaceConfig()` |
| `/api/spaces/config` | POST | Update space config | ✅ `kb.SetConfig()` | ✅ `agent.UpdateSpaceConfig()` |
| `/api/spaces/vectorize` | POST | Vectorize entire space | ✅ `db.Vectorize()` | ✅ `agent.VectorizeSpace()` |

### 7.3 Space Category Operations

| Endpoint | Method | Purpose | Current Implementation | Typed API Target |
|----------|--------|---------|------------------------|------------------|
| `/api/spaces/categories` | GET | List categories | ✅ `kb.ListCategories()` | ✅ `agent.ListCategories()` |
| `/api/spaces/category/add` | POST | Add single category | ✅ `kb.UpsertCategories([1])` | ✅ `agent.UpsertCategory()` |
| `/api/spaces/category/delete` | POST | Delete single category | ✅ `kb.DeleteCategories([1])` | ✅ `agent.DeleteCategory()` |

**MISSING Endpoints (Need to Add)**:
- ❌ `/api/spaces/categories/bulk` → `agent.BulkUpsertCategories()`
- ❌ `/api/spaces/categories/bulk/delete` → `agent.BulkDeleteCategories()`
- ❌ `/api/spaces/categories/vectorize` → `agent.BulkVectorizeCategories()`

### 7.4 Space Item Operations

| Endpoint | Method | Purpose | Current Implementation | Typed API Target |
|----------|--------|---------|------------------------|------------------|
| `/api/spaces/items` | GET | List items | ✅ `kb.ListItems()` | ✅ `agent.ListItems()` |
| `/api/spaces/item/add` | POST | Add single item | ✅ `kb.UpsertItems([1])` | ✅ `agent.UpsertItem()` |
| `/api/spaces/item/update` | POST | Update single item | ✅ `kb.UpsertItems([1])` | ✅ `agent.UpsertItem()` |
| `/api/spaces/item/delete` | POST | Delete single item | ✅ `kb.DeleteItems([1])` | ✅ `agent.DeleteItem()` |

**MISSING Endpoints (Need to Add)**:
- ❌ `/api/spaces/items/bulk` → `agent.BulkUpsertItems()`
- ❌ `/api/spaces/items/bulk/delete` → `agent.BulkDeleteItems()`
- ❌ `/api/spaces/items/vectorize` → `agent.BulkVectorizeItems()`

### 7.5 Space Import/Export/Ingest (Special Operations)

| Endpoint | Method | Purpose | Current Implementation | Status |
|----------|--------|---------|------------------------|--------|
| `/api/spaces/export` | POST | Export space to JSON | ✅ `kb.ExportJSON()` | ✅ Complete |
| `/api/spaces/import` | POST | Import space from JSON | ✅ `kb.ImportJSON()` | ✅ Complete |
| `/api/spaces/preload` | POST | Preload space with content | ✅ Custom handler | ✅ Complete |
| `/api/spaces/ingest` | POST | Ingest documents | ✅ Document parsing + UpsertItems | ✅ Complete |
| `/api/spaces/ingest/import` | POST | Ingest from file upload | ✅ Multipart upload + ingest | ✅ Complete |

**Notes**: 
- Export/Import are **specialized operations** for backup/restore
- Ingest is **content processing pipeline** (parse → chunk → embed → store)
- These are **complementary** to typed CRUD APIs, not duplicates

### 7.6 AI Chat & Agent Operations (Ask Pipeline)

| Endpoint | Method | Purpose | Implementation | Status |
|----------|--------|---------|----------------|--------|
| `/api/ai/chat` | POST | Ask AI agent (main entry) | ✅ handleAIChat → agentSvc.Ask() | ✅ Complete |
| `/api/ai/session/close` | POST | Close agent session | ✅ Session cleanup | ✅ Complete |
| `/api/ai/feedback` | POST | Submit feedback | ✅ Feedback logging | ✅ Complete |
| `/api/ai/summarize` | POST | Summarize content | ✅ LLM summarization | ✅ Complete |
| `/api/ai/test-connection` | GET | Test LLM connection | ✅ Generator health check | ✅ Complete |
| `/api/ai/test-embedder-connection` | GET | Test embedder connection | ✅ Embedder health check | ✅ Complete |
| `/api/tool/execute` | POST | Execute specific AI tool | ✅ Tool registry lookup | ✅ Complete |

**Key Flow**: 
```
HTTP Request → handleAIChat() 
  → resolveContextAndAgent()
  → lockSession() 
  → constructPayload()
  → executeAgentLifecycle() (agentSvc.Open())
  → executeRAG() (agentSvc.Ask())
  → agentSvc.Close()
```

### 7.7 Script Operations

| Endpoint | Method | Purpose | Implementation | Status |
|----------|--------|---------|----------------|--------|
| `/api/scripts/execute` | POST | Execute automation script | ✅ handleExecuteScript → agent.ExecuteScript() | ✅ Complete |

**Script Execution Flow**:
1. Load script from model store
2. Resolve parameters (template substitution)
3. Execute steps sequentially
4. Handle transaction mode: `none`, `single`, `per_step`
5. Return output variables + status

### 7.8 Transaction Operations (MISSING - Need to Add)

| Endpoint | Method | Purpose | Typed API Target | Status |
|----------|--------|---------|------------------|--------|
| ❌ `/api/transaction/begin` | POST | Start transaction | `agent.BeginTransaction()` | 🎯 To Add |
| ❌ `/api/transaction/commit` | POST | Commit transaction | `agent.CommitTransaction()` | 🎯 To Add |
| ❌ `/api/transaction/rollback` | POST | Rollback transaction | `agent.RollbackTransaction()` | 🎯 To Add |

### 7.9 Database & Configuration Operations

| Endpoint | Method | Purpose | Status |
|----------|--------|---------|--------|
| `/api/databases` | GET/POST/DELETE | Manage databases | ✅ Complete |
| `/api/databases/update` | POST | Update database config | ✅ Complete |
| `/api/db/options` | GET | Get database options | ✅ Complete |
| `/api/db/init` | POST | Initialize new database | ✅ Complete |
| `/api/config/save` | POST | Save server config | ✅ Complete |
| `/api/config/environments` | GET | List config environments | ✅ Complete |
| `/api/config/environments/create` | POST | Create environment | ✅ Complete |
| `/api/config/environments/switch` | POST | Switch environment | ✅ Complete |
| `/api/config/llm/update` | POST | Update LLM config | ✅ Complete |
| `/api/admin/validate` | POST | Validate admin token | ✅ Complete |
| `/api/tasks/status` | GET | Get async task status | ✅ Complete |

---

## Layer 6: Ask Pipeline (AI Agent Orchestration)

### 6.1 Ask Pipeline Components

**Location**: `tools/httpserver/main.ai.go`

```go
// Main entry point
handleAIChat(w, r) → agentSvc.Ask(ctx, message, options...)

// Pipeline steps
initializeRequest()      // Parse HTTP request → aiChatRequest
setupStream()            // NDJSON event streaming
resolveContextAndAgent() // Lookup agent from registry
lockSession()            // Session mutex + agent cloning
constructPayload()       // Build ai.SessionPayload with DB/KB context
executeAgentLifecycle()  // agentSvc.Open() + meta-cognition seeding
executeRAG()             // Core RAG: Retrieve → Generate → Augment
interpretOutput()        // Parse response (JSON, NDJSON, markdown)
```

### 6.2 Integration Points

- **Agents**: Uses `loadedAgents[agentName]` registry
- **Databases**: Injects `payload.CurrentDB` for context
- **Spaces**: Uses `payload.SelectedKBs` for RAG scope
- **Sessions**: Thread-safe via `activeSessions.GetOrCreate()`
- **Streaming**: NDJSON events for real-time UI updates
- **Tools**: Agent executes tools via `agent.Execute(ctx, tool, args)`

### 6.3 Response Formats

1. **Stream Mode** (default): NDJSON events
   - `data: {"type": "content", "content": "text chunk"}`
   - `data: {"type": "tool_start", "tool": "add", "args": {...}}`
   - `data: {"type": "tool_result", "result": "..."}`

2. **Non-Stream Mode**: JSON response
   ```json
   {"response": "...", "session_id": "..."}
   ```

---

## Layer 5: Script API (Automation)

### 5.1 Script Structure

**Location**: `ai/interfaces.go`

```go
type Script struct {
    Description     string
    Parameters      []string
    Database        string
    Portable        bool
    TransactionMode string // "none", "single", "per_step"
    Steps           []ScriptStep
}

type ScriptStep struct {
    Type     string // "ask", "set", "if", "loop", "fetch", "call_script"
    Agent    string
    Tool     string
    Arguments map[string]any
    OutputVariable string
    Conditional    string // CEL expression
    Then    []ScriptStep
    Else    []ScriptStep
}
```

### 5.2 Transaction Modes (Already Supported!)

| Mode | Behavior | Use Case |
|------|----------|----------|
| `none` | Manual transaction management | Script manages tx explicitly |
| `single` | ONE transaction for all steps | Atomic multi-step operations |
| `per_step` | Auto-commit after each step | Independent step execution |

### 5.3 Execution Flow

**Location**: `ai/agent/copilottools.automation.go`

```go
func (a *CopilotAgent) ExecuteScript(ctx, scriptName, params) {
    // 1. Load script from model store
    script := loadScript(scriptName)
    
    // 2. Handle transaction mode
    var tx sop.Transaction
    if script.TransactionMode == "single" {
        tx = beginTransaction(ctx)
        defer tx.Rollback(ctx)
    }
    
    // 3. Execute steps sequentially
    for _, step := range script.Steps {
        switch step.Type {
        case "ask":
            // Call LLM
        case "fetch", "add", "update":
            // Call tools (uses typed API if available)
        case "if":
            // Conditional branching (CEL)
        case "loop":
            // Iteration
        }
    }
    
    // 4. Commit if single mode
    if script.TransactionMode == "single" {
        tx.Commit(ctx)
    }
}
```

### 5.4 Integration with Typed APIs

**Current**: Scripts call tools with `map[string]any`  
**Target**: Tool layer converts to typed structs, delegates to typed API

```go
// Tool adapter pattern
func (a *CopilotAgent) toolAdd(ctx, args map[string]any) (string, error) {
    var typed AddArgs
    mapToStruct(args, &typed)       // Convert map → struct
    result, err := a.Add(ctx, typed) // Delegate to typed API
    return result, err
}
```

---

## Layer 4a: Typed Database API (NEW - Store Operations)

### 4.1 Single Operations

**Location**: `ai/agent/api_core.go`

| Method | Purpose | Status |
|--------|---------|--------|
| `Add(ctx, AddArgs)` | Add single item | ✅ Complete |
| `Update(ctx, UpdateArgs)` | Update single item | ✅ Complete |
| `Delete(ctx, DeleteArgs)` | Delete single item | ✅ Complete |
| `Select(ctx, SelectArgs)` | Query items | ✅ Complete |
| `Join(ctx, JoinArgs)` | Join stores | ✅ Complete |
| `ExecuteScript(ctx, ExecuteScriptArgs)` | Run script | ✅ Complete |

### 4.2 Bulk Operations

**Location**: `ai/agent/api_bulk.go`

| Method | Purpose | Transaction Modes | Status |
|--------|---------|-------------------|--------|
| `BulkAdd(ctx, BulkAddArgs)` | Add multiple items | auto_batch, single, explicit | ✅ Complete |
| `BulkUpdate(ctx, BulkUpdateArgs)` | Update multiple items | auto_batch, single, explicit | ✅ Complete |
| `BulkDelete(ctx, BulkDeleteArgs)` | Delete multiple items | auto_batch, single, explicit | ✅ Complete |

### 4.3 Transaction Management

**Location**: `ai/agent/api_transaction.go`

| Method | Purpose | Status |
|--------|---------|--------|
| `BeginTransaction(ctx, TransactionArgs)` | Start transaction | ✅ Complete |
| `CommitTransaction(ctx, TransactionCommitArgs)` | Commit transaction | ✅ Complete |
| `RollbackTransaction(ctx, TransactionRollbackArgs)` | Rollback transaction | ✅ Complete |

### 4.4 Integration Status

- **LLM Tools**: ⚠️ Need thin adapters (map[any] → structs)
- **HTTP Endpoints**: ⚠️ Need to add bulk endpoints
- **Scripts**: ✅ Compatible with TransactionMode
- **OpenAPI**: ✅ Annotations complete

---

## Layer 4b: Typed Space API (NEW - Knowledge Base Operations)

### 4.1 Space Lifecycle

**Location**: `ai/agent/api_space.go`

| Method | Purpose | Status |
|--------|---------|--------|
| `CreateSpace(ctx, CreateSpaceArgs)` | Create/open space | ✅ Complete |
| `DeleteSpace(ctx, DeleteSpaceArgs)` | Delete space | ✅ Complete |
| `UpdateSpaceConfig(ctx, UpdateSpaceConfigArgs)` | Update config | ✅ Complete |
| `ReadSpaceConfig(ctx, ReadSpaceConfigArgs)` | Read config | ✅ Complete |
| `VectorizeSpace(ctx, VectorizeSpaceArgs)` | Vectorize all | ✅ Complete |

### 4.2 Category Operations

**Single**:
| Method | Purpose | Status |
|--------|---------|--------|
| `UpsertCategory(ctx, UpsertCategoryArgs)` | Upsert single category | ✅ Complete |
| `DeleteCategory(ctx, DeleteCategoryArgs)` | Delete single category | ✅ Complete |
| `ListCategories(ctx, ListCategoriesArgs)` | List with pagination | ✅ Complete |

**Bulk** (Location: `ai/agent/api_space_bulk.go`):
| Method | Purpose | Status |
|--------|---------|--------|
| `BulkUpsertCategories(ctx, BulkUpsertCategoriesArgs)` | Upsert multiple | ✅ Complete |
| `BulkDeleteCategories(ctx, BulkDeleteCategoriesArgs)` | Delete multiple | ✅ Complete |
| `BulkVectorizeCategories(ctx, BulkVectorizeCategoriesArgs)` | Vectorize multiple | ✅ Complete |

### 4.3 Item Operations

**Single**:
| Method | Purpose | Status |
|--------|---------|--------|
| `UpsertItem(ctx, UpsertItemArgs)` | Upsert single item | ✅ Complete |
| `DeleteItem(ctx, DeleteItemArgs)` | Delete single item | ✅ Complete |
| `ListItems(ctx, ListItemsArgs)` | List with pagination | ✅ Complete |
| `SearchItemsByPath(ctx, SearchItemsByPathArgs)` | Search by path | ✅ Complete |

**Bulk**:
| Method | Purpose | Status |
|--------|---------|--------|
| `BulkUpsertItems(ctx, BulkUpsertItemsArgs)` | Upsert multiple | ✅ Complete |
| `BulkDeleteItems(ctx, BulkDeleteItemsArgs)` | Delete multiple | ✅ Complete |
| `BulkVectorizeItems(ctx, BulkVectorizeItemsArgs)` | Vectorize multiple | ✅ Complete |

### 4.4 Compilation Status

✅ **All compilation errors fixed!** Space API compiles successfully (verified June 2, 2026)

---

## Layer 4c: JSON Store API (PUBLIC - Language Bindings)

### 4.1 StoreAccessor Interface

**Location**: `jsondb/opener.go`

```go
type StoreAccessor interface {
    // Navigation
    First(ctx) (bool, error)
    Last(ctx) (bool, error)
    Next(ctx) (bool, error)
    Previous(ctx) (bool, error)
    
    // Search
    FindOne(ctx, key any, first bool) (bool, error)
    FindInDescendingOrder(ctx, key any) (bool, error)
    
    // Access
    GetCurrentKey() any
    GetCurrentValue(ctx) (any, error)
    
    // Modification
    Add(ctx, key any, value any) (bool, error)
    Update(ctx, key any, value any) (bool, error)
    Remove(ctx, key any) (bool, error)
    
    // Metadata
    GetStoreInfo() sop.StoreInfo
}
```

### 4.2 Store Types

| Type | Key Type | Value Type | Use Case |
|------|----------|------------|----------|
| **Primitive Store** | `any` (typically string) | `any` | Simple key-value |
| **JSON Store** | `map[string]any` | `any` | Complex keys with IndexSpecification |

### 4.3 Language Bindings

| Language | Status | Package |
|----------|--------|---------|
| **Go** | ✅ Complete | `jsondb` |
| **Rust** | ✅ Complete | `sop-rs` |
| **C#** | ✅ Complete | `Sop.dll` |
| **Java** | ✅ Complete | `sop.jar` |
| **Python** | ✅ Complete | `sop-py` |

### 4.4 Relationship to Typed APIs

```
Typed Database API (agent.Add)
    ↓ converts args to map[string]any
    ↓ calls jsondb.OpenStore()
    ↓
JSON Store API (store.Add)
    ↓ delegates to
    ↓
Core B-Tree API (btree.Add)
```

**Design Decision**: JSON Store API is the **low-level public API** for language bindings. Typed APIs are **high-level convenience wrappers** for Go applications.

---

## Layer 3: AI Database API

**Location**: `ai/database/database.go`

### 3.1 Store Management

```go
func (db *Database) OpenBtree(ctx, name, tx) → btree.BtreeInterface[string, any]
func (db *Database) NewBtree(ctx, name, tx) → btree.BtreeInterface[string, any]
func (db *Database) OpenBtreeCursor(ctx, name, tx) → btree.BtreeInterface[string, any]
```

### 3.2 AI-Specific Stores

```go
func (db *Database) OpenModelStore(ctx, name, tx) → ai.ModelStore
func (db *Database) OpenVectorStore(ctx, name, tx, cfg) → ai.VectorStore
func (db *Database) OpenSearch(ctx, name, tx) → *search.Index
func (db *Database) OpenKnowledgeBase(ctx, name, tx, brain, embedder) → *KnowledgeBase
```

### 3.3 Management

```go
func (db *Database) GetStores(ctx) → []string
func (db *Database) GetDomains(ctx) → []string (Spaces)
func (db *Database) RemoveKnowledgeBase(ctx, name) → error
```

---

## Layer 2: Space Storage (KnowledgeBase CRUD)

**Location**: `ai/memory/knowledge_base_crud.go`

### 2.1 Category Operations

```go
func (kb *KnowledgeBase) UpsertCategories(ctx, []UpsertCategoryParam) → error
func (kb *KnowledgeBase) DeleteCategories(ctx, []UUID) → error
func (kb *KnowledgeBase) ListCategories(ctx, ListCategoriesParam) → ([]Category, int, error)
```

### 2.2 Item Operations

```go
func (kb *KnowledgeBase) UpsertItems(ctx, []UpsertItemParam[T]) → error
func (kb *KnowledgeBase) DeleteItems(ctx, []ItemKey) → error
func (kb *KnowledgeBase) ListItems(ctx, ListItemsParam) → ([]Item[T], int, error)
func (kb *KnowledgeBase) SearchByPath(ctx, []PathSearchParam) → ([]Item[T], error)
```

### 2.3 Import/Export

```go
func (kb *KnowledgeBase) ExportJSON(ctx) → ([]byte, error)
func (kb *KnowledgeBase) ImportJSON(ctx, []byte) → error
```

---

## Layer 1: Core B-Tree API (Foundation)

**Location**: `database/database.go`, `common/managebtree.go`

### 1.1 Store Management

```go
func database.NewBtree[TK, TV](ctx, config, name, tx, comparer) → BtreeInterface[TK, TV]
func database.OpenBtree[TK, TV](ctx, config, name, tx, comparer) → BtreeInterface[TK, TV]
func database.OpenBtreeCursor[TK, TV](ctx, config, name, tx, comparer) → BtreeInterface[TK, TV]
func database.RemoveBtree(ctx, config, name) → error
func database.RemoveBtrees(ctx, config) → error
```

### 1.2 Transaction Management

```go
func database.BeginTransaction(ctx, config, mode) → Transaction
func database.Setup(ctx, config) → (Database, error)
```

### 1.3 B-Tree Interface

```go
type BtreeInterface[TK, TV] interface {
    Add(ctx, key TK, value TV) (bool, error)
    Update(ctx, key TK, value TV) (bool, error)
    Remove(ctx, key TK) (bool, error)
    Find(ctx, key TK, first bool) (bool, error)
    First(ctx) (bool, error)
    Next(ctx) (bool, error)
    GetCurrentKey() ItemReference[TK]
    GetCurrentValue(ctx) (TV, error)
    // ... more methods
}
```

---

## Integration Summary

### What's Complete ✅

1. **Core B-Tree API** - Foundation layer
2. **JSON Store API** - Public bindings to 5 languages
3. **AI Database API** - AI-specific wrappers
4. **Space Storage** - KnowledgeBase CRUD
5. **Ask Pipeline** - AI chat orchestration
6. **Script API** - Automation with transaction modes
7. **Typed Database API** - Type-safe Store operations
8. **HTTP Endpoints** - 45+ REST endpoints deployed

### What's Incomplete ⚠️

1. **Typed Space API** - 90% done, has compilation errors
2. **HTTP ↔ Typed API Integration** - Endpoints don't use typed APIs yet
3. **Bulk Endpoints** - Missing for both Stores and Spaces
4. **Transaction Endpoints** - No HTTP endpoints for tx management
5. **LLM Tool Adapters** - Need thin wrappers for typed APIs

### What's Missing ❌

1. **HTTP Bulk Endpoints**:
   - `/api/store/items/bulk`
   - `/api/store/items/bulk/update`
   - `/api/store/items/bulk/delete`
   - `/api/spaces/categories/bulk`
   - `/api/spaces/items/bulk`

2. **HTTP Transaction Endpoints**:
   - `/api/transaction/begin`
   - `/api/transaction/commit`
   - `/api/transaction/rollback`

3. **Documentation**:
   - OpenAPI spec for typed APIs
   - HTTP endpoint migration guide
   - Integration patterns document

---

## Roadmap to Connect All Dots

### Phase 1: Fix Space API (IMMEDIATE)
**Duration**: 1-2 hours

1. Fix compilation errors in `api_space.go` and `api_space_bulk.go`
2. Align generic types with `memory` package
3. Fix method signatures (`DeleteItems`, `SearchByPath`)
4. Verify all code compiles
5. Test basic operations

### Phase 2: HTTP Endpoint Integration (SHORT-TERM)
**Duration**: 1 day

1. Add bulk operation endpoints (6 new routes)
2. Add transaction management endpoints (3 new routes)
3. Refactor existing endpoints to use typed APIs
4. Add request/response examples
5. Update HTTP tests

### Phase 3: LLM Tool Integration (SHORT-TERM)
**Duration**: 2 hours

1. Create thin adapter pattern for all tools
2. Refactor `toolAdd`, `toolUpdate`, `toolDelete`
3. Refactor `toolUpsertCategories`, `toolUpsertItems`
4. Test tool execution via scripts
5. Test tool execution via AI chat

### Phase 4: Documentation (SHORT-TERM)
**Duration**: 1 day

1. Generate complete OpenAPI spec
2. Create integration guide with examples
3. Document transaction patterns
4. Create migration guide for existing users
5. Update AI_COPILOT_USAGE.md

### Phase 5: Testing & Validation (MEDIUM-TERM)
**Duration**: 2-3 days

1. Integration tests for HTTP → Typed API flow
2. Transaction isolation tests
3. Bulk operation performance tests
4. Script execution with transaction modes
5. End-to-end workflow tests

---

## Decision Log

### ✅ Decisions Made

1. **JSON Store API remains public** - Language bindings use it
2. **Typed APIs are high-level wrappers** - For Go applications
3. **Scripts have transaction support** - Already implemented
4. **Import/Export are specialized** - Not duplicates of CRUD
5. **Ask Pipeline is complete** - No changes needed

### 🎯 Decisions Needed

1. **Deprecation plan for direct DB access in HTTP endpoints?**
   - Option A: Gradual migration (recommended)
   - Option B: Break compatibility in v2.0
   - Option C: Maintain both forever

2. **Should Scripts auto-use typed APIs?**
   - Option A: Tool adapters delegate (recommended)
   - Option B: Keep direct DB access
   - Option C: Make it configurable

3. **OpenAPI spec generation strategy?**
   - Option A: Use swag annotations (current)
   - Option B: Runtime reflection
   - Option C: Both

---

## Summary: No Duplication, Just Integration

**Key Insight**: We don't have duplication - we have **complementary layers** that need **proper integration**.

- **JSON Store API** = Low-level, public, language bindings
- **Typed Database API** = High-level, type-safe, Go applications
- **Typed Space API** = High-level, knowledge-specific, Go applications
- **HTTP REST API** = Public endpoints for end users
- **Ask Pipeline** = AI orchestration, uses all layers
- **Script API** = Automation, transaction-aware

**Next Step**: Fix Space API compilation errors, then integrate HTTP endpoints with typed APIs.

**Would you like me to proceed with Phase 1 (Fix Space API)?**
