# AI Copilot User Guide

The SOP AI Copilot is a powerful, conversational interface for interacting with your SOP databases. It allows you to query data, perform CRUD operations, and automate complex workflows using natural language.

## Core Philosophy: Stateless vs. Stateful

To ensure system stability and prevent "dangling transactions," the Copilot operates in two distinct modes:

1.  **Stateless (Interactive & Recording)**: Every prompt is an independent unit of work.
    *   If you ask "Select all users", the Copilot opens a transaction, reads the data, and **immediately closes** the transaction.
    *   This applies even when **Recording**. Each step you record is executed and committed immediately.
2.  **Stateful (Playback)**: When **Playing a Script**, the Copilot can maintain a transaction across multiple steps.
    *   This allows scripts to perform complex, multi-step atomic operations (e.g., "Transfer funds: Debit A, Credit B").
    *   If any step fails, the entire script transaction can be rolled back.

---

## 1. Natural Language Data Access

You can ask the Copilot to retrieve data using plain English. It translates your intent into optimized B-Tree lookups.

### Listing Resources
*   **"Show me all databases"** -> Lists available databases.
*   **"What stores are in the 'users' database?"** -> Lists tables/stores in that DB.

### Selecting Data
The `select` tool is powerful and supports filtering and field selection.

*   **Basic**: "Get the first 10 records from the 'users' store."
*   **Filtering**: "Find users in the 'users' store where the 'role' is 'admin'." Supports MongoDB-style operators for comparisons: `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$eq`. Example: "Select employees where age > 30" (Assistant converts this to `{"age": {"$gt": 30}}`).
*   **Field Selection**: "Show me just the 'username' and 'email' for all users."
*   **Scanning**: "Scan the 'logs' store for entries with 'error' in the message." (Note: Scanning large stores can be slow; prefer key lookups).
*   **Ordering**: SOP stores are B-Trees and are naturally ordered by their Keys. Therefore, explicit `ORDER BY` clauses are not supported (and not needed). You always operate in the native B-Tree sort order.
*   **UI Display Note**: When selecting specific fields, the backend returns them in the requested order (e.g., `select salary, name` returns `salary` then `name`). However, the **UI Grid** always displays Key fields (columns from the Key object) *before* Value fields (columns from the Value object) for consistency. If you request a Value field followed by a Key field, they will appear as Key then Value in the grid. The raw JSON response (accessible via API) preserves your requested order within the Key and Value objects respectively.
*   **Views (Scripts)**: You can use a Script as a data source! If you have a script named 'active_users_view' that returns a list of users, you can query it like a table: "Select name, email from 'active_users_view'". This allows you to create complex "Views" using scripts (even with Joins) and query them simply. **Streaming Support**: Unlike traditional views that might materialize results, SOP streams script output directly. Field selection is applied "late-bound" as items flow through, ensuring high efficiency even for complex pipelines.

### Finding Specific Records
*   **Exact Match**: "Find the user with key 'user_123'."
*   **Nearest Match**: "Find the user closest to 'user_125'." (Useful for finding range boundaries).

## Querying Rules & Expectations

To ensure accurate results, it's important to understand how the AI Copilot and API interpret your queries, particularly regarding case sensitivity.

### Schema Names (Case-Insensitive)
Field names and JSON keys are **Case-Insensitive**.
*   Querying for `field: "Name"` will successfully match a field named `name`, `NAME`, or `Name`.
*   This flexibility allows you to ask natural questions without needing to know the exact capitalization of the schema.
*   **Note**: While the query engine is flexible, the returned data will preserve the original casing stored in the database.

### Data Values (Case-Sensitive)
Actual data values used in comparisons are **Case-Sensitive**.
*   **Search**: Searching for `role: "Admin"` will **NOT** match a record where the role is `"admin"`.
*   **Joins**: When joining two stores, the values in the joining fields must match exactly (e.g., ID `"u123"` will not join with `"U123"`).
*   **Best Practice**: Ensure your query values match the case of the data stored in the database.

### Entity Names (Case-Insensitive)
Names of Databases, Stores, and Scripts are **Case-Insensitive**.
*   You can refer to your store as 'Users', 'users', or 'USERS', and the Copilot will find the correct one.
*   **Note**: In the rare case where two stores have the same name but different casing (e.g. `users` and `USERS`), the system prefers the exact match.

### Efficient Query Scenarios

SOP is a high-performance database that uses B-Trees. To get the maximum speed (especially on large datasets), structure your questions to leverage the Index (Key) structure.

**How to write fast queries:**
The "Index" is defined by the Key fields of your store.
*   **Fast**: Filtering by the *first* field(s) of the Key (Prefix Match).
*   **Fast**: Joining on the Key fields.
*   **Slow**: Filtering by a field that is *not* at the start of the Key (requires a full scan).

| Scenario | Index (Key) Structure | Query Example | Status |
| :--- | :--- | :--- | :--- |
| **Exact Match** | `[Region, Dept]` | "Find employees in 'US' 'Sales'" | ⚡ **Fast** |
| **Prefix Match** | `[Region, Dept]` | "Find employees in 'US'" | ⚡ **Fast** |
| **Natural Sort** | `[Region, Dept]` | "Find 'US' employees, ordered by Dept" | ⚡ **Fast** |
| **Skip Prefix** | `[Region, Dept]` | "Find employees in 'Sales'" (Skipped Region) | 🐢 **Slow** (Full Scan) |
| **Sort Conflict** | `[Region, Dept]` | "Find 'US' employees, ordered by Dept DESC" (If index is ASC) | 🐢 **Slower** (Buffered) |

---

## 2. CRUD Operations

You can modify data directly.

### Adding Data
*   **"Add a new user to 'users' with key 'u_99' and name 'Alice'."**
*   **"Insert this JSON into 'config': {...}"**

### Updating Data
*   **"Update user 'u_99' and set 'status' to 'active'."**
*   **"Change the email for 'u_99' to 'alice@example.com'."**

### Deleting Data
*   **"Delete the record 'u_99' from 'users'."**

> **Note**: In **Stateless Mode**, these operations commit immediately. If you need to do multiple updates atomically, consider using a Script.

---

## 2.1 Bulk Operations & Typed Database API

> **Note**: This section covers the **Database Operations API** for B-Trees and bulk CRUD. For AI memory operations (Spaces/Knowledge Bases), see [Section 3: Spaces and Knowledge Bases](#3-spaces-and-knowledge-bases).

For high-performance bulk database operations (10K+ items), SOP provides a strongly typed API with automatic transaction batching.

### Why Bulk Operations?

Single-item CRUD operations commit immediately in interactive mode. For large datasets:
- **100 items**: Individual operations work fine
- **10,000+ items**: Use bulk operations with automatic batching
- **Transaction control**: Choose between scalability and atomicity

### Bulk Operations Available

#### bulk_add
Insert multiple items efficiently with automatic transaction batching.

**Natural Language:**
- "Bulk insert 50,000 user records with auto-batching"
- "Add these 10,000 items to the users store using single transaction mode"

**Typed API (Go):**
```go
result, err := agent.BulkAdd(ctx, agent.BulkAddArgs{
    Store: "users",
    Items: items,  // []BulkItem with key/value pairs
    TransactionMode: agent.TransactionModeAutoBatch,  // or Single, Explicit
    BatchSize: 250,  // Items per batch
})

// Result includes metrics
fmt.Printf("Processed: %d, Failed: %d, Duration: %s\n", 
    result.Processed, result.Failed, result.Duration)
fmt.Printf("Items/sec: %.0f, Batches: %d\n",
    result.Metrics.ItemsPerSecond, result.Metrics.BatchesExecuted)
```

#### bulk_update / bulk_delete
Similar to bulk_add but for updates and deletes.

### Transaction Modes

| Mode | Use Case | Items | Behavior | Atomicity |
|------|----------|-------|----------|-----------|
| **auto_batch** | Large bulk ops | 10K+ | Tx per batch, auto-commit | Per-batch |
| **single** | Atomic bulk | <10K | ONE tx, single commit | All-or-nothing |
| **explicit** | Multi-op atomic | Any | Use provided tx, caller commits | Cross-operation |

**auto_batch (Default - Scalability)**
```
Batch 1: Begin → Add 250 items → Commit
Batch 2: Begin → Add 250 items → Commit
...
Batch 40: Begin → Add 250 items → Commit
```
- ✅ Scales to millions of items
- ✅ Memory efficient (streaming)
- ⚠️ Partial success possible (batch-level atomicity)

**single (Atomicity)**
```
Begin → Add ALL 5,000 items → Commit
```
- ✅ All-or-nothing guarantee
- ✅ Perfect for financial operations
- ⚠️ Limited to ~10K items (memory)

**explicit (Multi-Operation Atomicity)**
```
tx = Begin Transaction
Bulk Add 1,000 users with tx
Bulk Update 500 profiles with tx
Bulk Delete 200 old records with tx
Commit Transaction
```
- ✅ Atomic across multiple operations
- ✅ Full control over transaction lifecycle
- ⚠️ Caller must manage commit/rollback

### OpenAPI & Schema-Driven Accuracy

The typed API generates OpenAPI schemas that serve as the **single source of truth** for LLM guidance.

**Traditional Approach (Verbose, Error-Prone):**
```go
// 150+ lines of inline schema definitions
registry.Register("bulk_add", "description", map[string]any{
    "type": "object",
    "properties": map[string]any{
        "store": map[string]any{"type": "string", ...},
        "items": map[string]any{"type": "array", ...},
        // ... 100+ more lines
    },
}, toolBulkAdd)
```
❌ Schema drifts from implementation  
❌ Verbose, hard to maintain  
❌ No compile-time validation  

**OpenAPI Approach (Concise, Always Accurate):**
```markdown
# LLM Guidance (10 lines vs 150+)
Tools: bulk_add, bulk_update, bulk_delete
Schemas: ai/agent/docs/schemas/*.json

bulk_add - Insert multiple items
  Required: store, items
  Recommended: transaction_mode="auto_batch", batch_size=250
```
✅ **93% reduction** in guidance size  
✅ **Single source** - Schemas generated from Go structs  
✅ **Always accurate** - Can't drift (auto-generated)  
✅ **Type-safe** - Compile-time validation  

**Benefits:**
- **LLM Accuracy**: Reference concise schemas instead of verbose inline definitions
- **Multi-Language**: Generate clients for Python, TypeScript, Rust, Java, C#
- **Test Harnesses**: Use strongly typed API directly in tests
- **HTTP Endpoints**: Auto-validated requests against OpenAPI spec
- **Documentation**: Swagger UI with interactive testing

See [ai/agent/README_API.md](../agent/README_API.md) for complete API documentation.

---

## 3. Spaces and Knowledge Bases

> **Architecture Note**: Spaces have their own **domain-specific API** separate from the database operations API covered in Section 2.1. These are complementary systems serving different purposes.

The AI acts as an **Omni Persona**—managing both underlying B-Trees as a Database Engineer and recognizing "Spaces" or "Knowledge Bases" explicitly.

A "Space" or "Knowledge Base" (often represented as a single word like "Notes" or "Contacts") is an AI memory subsystem comprised of a VectorDB, Text Search, and a special schema (Thoughts: Category/Items) along with its memory management. When a user asks to generate, translate, upload, or import data into a target Space, the AI **DOES NOT USE ANY RAW DATABASE TOOLS** (e.g., `add`, `bulk_add`, `execute_script`). Treating Spaces like raw B-Trees will result in schema validation errors.

### Spaces API vs Database API

**Use Spaces API for:**
- ✅ Knowledge bases, notes, contacts
- ✅ Semantic search and embeddings
- ✅ AI memory with categories/items
- ✅ Tools: `mint_to_space`, `enrich_space`, `vectorize_space`

**Use Database API (Section 2.1) for:**
- ✅ Raw B-Tree operations
- ✅ Bulk CRUD (10K+ items)
- ✅ Transaction control
- ✅ Tools: `bulk_add`, `bulk_update`, `bulk_delete`, `select`, `execute_script`

The Spaces API is **high-level** (AI-oriented), while the Database API is **low-level** (storage-oriented). 

Instead, the AI should use its Space APIs such as `upsert_space_items` natively to manage the categories and items, ensuring correct memory ingestion sequences.

### Advanced KB Routing: Prefix-Based Navigation

SOP supports powerful **prefix-based routing** to directly navigate and query knowledge bases using a specialized syntax. This provides deterministic, fast access to hierarchical knowledge without relying on conversational interpretation.

#### Basic Routing Syntax

```
omni:<knowledge_base>                         # Display root categories
omni:<knowledge_base>:<category_path>        # Navigate to specific category
```

**Root Category Exploration:**
```
omni:sop                    # Show all root categories in SOP KB
omni:medical                # Show all root categories in Medical KB
omni:myapp                  # Show all root categories in custom KB
```

**Response example for `omni:sop`:**
```
Available Categories:

• Language (150 items, 5 subcategories)
  Programming language guides and tutorials
  Navigate: omni:sop:language

• Architecture (89 items, 3 subcategories)
  System design and architecture patterns
  Navigate: omni:sop:architecture

• Operations (203 items, 7 subcategories)
  DevOps, deployment, and operational guides
  Navigate: omni:sop:operations
```

**Pagination:** Displays show 20 categories per page. To navigate (both `:` and `/` separators work):
```bash
omni:sop               # Page 1 (default)
omni:sop:page:2        # Page 2  
omni:sop/page/3        # Page 3 (slash separator)
omni:sop:language:page:2   # Page 2 of subcategories
omni:sop/language/page/2   # Same, using slash separator
```

**Multi-page response example:**
```
Available Categories: (Page 2 of 5, showing 21-40 of 87)

• Category 21 (45 items, 2 subcategories)
  ...
• Category 40 (12 items)
  ...

Previous: omni:sop:page:1 | Next: omni:sop:page:3
💡 Tip: Use `omni:sop:llm list categories matching <name>` to filter results.
```

**Category Path Navigation:**
```
omni:sop:language bindings
omni:sop:operations:performance
omni:medical:diagnosis:cardiology:procedures
```

**How it works:**
- `omni:` signals specialized routing (Gate 1)
- `<knowledge_base>` specifies which KB to query (e.g., `sop`, `medical`, `myapp`)
- `<category_path>` uses colon-separated hierarchy at any depth

The system performs intelligent resolution:
1. **Root navigation** when only KB name is provided (displays all root categories)
2. **Direct lookup** via category path index (O(1) when exact match exists)
3. **Semantic fallback** using category embeddings when no exact match
4. **Subcategory navigation** when the category has no direct items

#### The `:llm <instruction>` Meta-Token

Add `:llm <instruction>` to have the AI process the retrieved results:

```
omni:sop:operations:performance:llm summarize
omni:sop:language bindings:c#:llm explain with code examples
omni:myapp:tutorials:beginner:llm list top 5 by popularity
```

**What happens:**
- The `:llm` portion is automatically stripped before KB search
- KB retrieval proceeds normally
- Results are passed to the AI along with your instruction
- The AI filters, summarizes, or analyzes the matches

**Real-world examples:**
```
omni:sop:operations:performance:caching:llm summarize the top 3
  → Searches caching category, AI summarizes top 3 results

omni:medical:treatments:cardiology:llm compare effectiveness
  → Retrieves cardiology treatments, AI compares them

omni:sop:architecture:patterns:llm explain pros and cons
  → Gets architecture patterns, AI analyzes trade-offs
```

#### Automatic Routing Decisions

The system makes intelligent decisions based on result count:

| Scenario | Behavior |
|----------|----------|
| **1-5 matches** | Direct display (no LLM processing) |
| **`:llm` present** | AI processes according to instruction |
| **6+ matches** | AI automatically summarizes |
| **No items found** | Shows subcategory navigation |

#### Subcategory Navigation

When a category has no direct items, you'll see subcategory hints:

```
Query: omni:sop:language bindings

Response:
📁 Category "language bindings" has no direct items.

Available subcategories (3):
  1. c# (12 items) - C# language binding documentation
  2. java (8 items) - Java integration guides
  3. python (15 items) - Python SDK reference

💡 Navigate deeper: omni:sop:language bindings:c#
```

This lets you browse the knowledge hierarchy like a directory tree.

#### Flexible Hierarchy Support

Any depth is supported:

```
✅ Single level:     omni:sop:operations
✅ Two levels:       omni:sop:operations:performance  
✅ Three levels:     omni:sop:operations:performance:caching
✅ Deep hierarchy:   omni:myapp:a:b:c:d:e:f:g
```

#### Combining with LLM Instructions

All patterns work together:

```
# Category navigation
omni:sop:language bindings:c#

# With LLM summarization
omni:sop:language bindings:c#:llm summarize

# Deep path with instruction
omni:medical:diagnosis:cardiology:procedures:stent:llm explain risks

# Complex instruction
omni:sop:architecture:microservices:llm compare with monolith architecture
```

#### Future: Quoted Text Search (Roadmap)

Coming soon - combined category + text search:

```
omni:sop:language bindings "java tutorial"
  → Search for "java tutorial" within language bindings category

omni:sop:operations:performance "caching strategies":llm top 3
  → Search for text, AI summarizes top 3 matches
```

**Syntax (proposed):**
- Category path before quotes: `omni:sop:operations:performance`
- Search text in quotes: `"caching strategies"`
- Optional LLM instruction: `:llm summarize top 3`

#### Quick Reference

| Pattern | Example | Use Case |
|---------|---------|----------|
| **Direct routing** | `omni:sop:operations` | Browse a category |
| **Deep path** | `omni:sop:ops:perf:cache` | Navigate deep hierarchy |
| **LLM filter** | `omni:sop:guides:llm top 5` | AI selects best matches |
| **LLM summarize** | `omni:sop:api:llm summarize` | AI condenses info |
| **LLM analyze** | `omni:sop:patterns:llm compare` | AI provides analysis |
| **Text search** | `omni:sop:docs "tutorial"` | *(Roadmap)* Search within category |

## 4. Memory & Learning: The Self-Correcting Copilot

The AI is designed to evolve. It possesses two distinct types of memory, allowing it to adapt to your specific environment and business logic over time.

### Short-Term Memory ( The "Session" Context)
This is the **Working Memory**. It holds the immediate context of your current conversation, active transaction, and recent query results.

*   **Duration**: Ephemeral. Cleared when you refresh or start a new session.
*   **Capabilities**:
    *   **"Peek-Ahead" Schema Awareness**: Before answering, the AI "peeks" at your real data to learn field names and types (e.g., seeing that `status` is an `int`, not a `string`). This prevents halogenations about your schema.
    *   **Conversational Continuity**: "Refine that last query", "Use the ID from the previous result".
*   **Privacy**: Session data is isolated. It doesn't leak into the global brain unless explicitly saved.

### Long-Term Memory (The "System Knowledge")
This memory architecture operates on a **generic blueprint**: **Expertise KB + Memory System (STM/LTM)**. 

Depending on the agent profile, it behaves as follows:
*   **Omni (The System Agent)**: Uses the **SOP KB (Shared Expertise)** for static architectural wisdom compiled from offline Markdown files + **Memory System**. Omni can additionally be given setups containing multiple custom KBs for data lookup.
*   **Avatars**: Utilize **Custom KBs** (specific domain knowledge) + **Memory System** for distinct LLM interactions.

This generic blueprint breaks down into two core engines with strict privacy scopes:
1. **Expertise KB (e.g. SOP KB)**: Houses static knowledge. SOP KB is shared across instances detailing tech stack operations.
2. **Memory System (Private STM/LTM KB)**: Persists live conversational facts, transient rules, and self-correction constraints privately per user.

*   **V1 Implementation**: Originally used an `llm_instructions` B-Tree in the `SystemDB` where the LLM explicitly managed knowledge via transactional tools (like `manage_knowledge`).
*   **V2 Storage (Current)**: Rules are now stored semantically via the Omni Protocol in dynamic Vector KBs in the `SystemDB`. The Butler Architecture separates hard instruction fetching (SOP KB) from interactive learning paths (LTM KB).
*   **The "Butler Architecture"**: The AI doesn't load *everything* into massive static prompts. It uses dynamic queries to automatically fetch exact capability parameters from SOP KB and actively checks the Memory System for session corrections.
*   **Self-Correction (One of many Omni capabilities)**:
    1.  **Passive Corrections**: If the AI makes a mistake and you correct it, the active context is seamlessly committed to the LTM KB without explicit tool calls.
    2.  **Structural Domain Rules**: Drop broad rules directly into domain `.md` files in the repository. Native compilation extracts them into `sop_base_knowledge.json`.
    3.  **Active Lookup**: Upon relevant conversational triggers, Copilot simultaneously consults both the SOP KB and LTM KB via `SearchKeywords` bypassing rigid prompt structures.

**Why this matters**: You don't need to write perfect monolithic prompts. The system dynamically tailors context to your exact situation seamlessly.

---

## 5. Script Management (Automation)

Scripts allow you to record a sequence of actions and replay them later. This is "Natural Language Programming."

### Recording a Script
1.  **Start**: Type `/record my_new_script`.
    *   **Default (Compiled)**: Records the exact *actions* (tools) you perform. When played back, it executes these actions directly (fast, deterministic).
    *   **Interactive Mode**: Type `/record my_new_script --ask`. Records your *prompts*. When played back, it asks the AI again (slower, but adapts to new data/context).
2.  **Teach**: Perform your actions step-by-step.
    *   "Select users where role is admin."
    *   "For each user, check their last login."
    *   "If last login is > 30 days, set status to inactive."
3.  **Stop**: Type `/stop`.

The script is saved **incrementally** after every step. If you crash or disconnect, your progress is safe!

### Playing a Script
*   **Run**: Type `/play my_new_script`.
*   **With Arguments**: If your script takes parameters (e.g., `user_id`), the Assistant will prompt you for them or you can provide them in the chat.

### Managing Scripts
*   **List**: "List all scripts."
*   **Inspect**: "Show me the steps for 'my_new_script'."
*   **Refactor**: If you just had a great conversation and want to save it as a script *after the fact*, say: "Refactor that last interaction into a script named 'audit_workflow'."

---

## 4. Advanced Features

### Explicit Transaction Management
For complex, multi-step manual operations without creating a script, you can manually control the transaction:
*   "Begin a transaction."
*   (Perform multiple updates...)
*   "Commit transaction."

### Swarm Computing (Async)
When defining scripts manually (or asking the Assistant to edit them), you can mark steps as `is_async: true`. This allows the Assistant to execute multiple heavy tasks in parallel (e.g., "Summarize these 50 documents").

---

## 5. Session Tools

*   **/last-tool**: Displays the exact JSON instructions (tool name and arguments) of the last executed action. Useful for debugging or verifying what the Assistant actually did.

---

### Example Session

```text
User: /record onboard_user
Copilot: Recording started for script 'onboard_user'.

User: Add a user to 'users' with key 'new_guy' and status 'pending'.
Copilot: Added user 'new_guy'. (Step recorded)

User: Add a log entry to 'audit_logs' saying "User new_guy created".
Copilot: Added log entry. (Step recorded)

User: /stop
Copilot: Recording stopped. Script 'onboard_user' saved with 2 steps.

User: /play onboard_user
Copilot: Executing 'onboard_user'...
1. Added user...
2. Added log entry...
```
---

## 6. Mobile & Small Device Support ("Pocket Admin")

The SOP AI Copilot is designed with a **Mobile-First** responsiveness philosophy, turning your smartphone or tablet into a full-featured "Pocket Admin" console.

### Optimized UI
The chat interface automatically adapts to smaller screens:
*   **Data Grids**: Collapse gracefully or allow horizontal scrolling without breaking the layout.
*   **Action Chips**: Suggested actions and database selectors are touch-friendly.
*   **Visual Feedback**: Loading states and success confirmations are designed for quick visual scanning on small screens.

### Full-Featured Management
You are not limited to "read-only" views on mobile. You can perform **all** system operations via chat:
*   **Switch Databases**: Tap the database selector or simply say "Switch to the 'finance' database" to instantly jump contexts.
*   **Run Complex Queries**: "Find all users joined today" works just as well on a phone as on a desktop.
*   **Execute Scripts**: Trigger complex backend jobs (e.g., "Run end-of-day reconciliation") with a single message while commuting.
*   **Data Rescue**: Need to fix a record urgently? "Update user '123', set status to 'active'" allows for emergency interventions from anywhere.

The Copilot essentially acts as a highly capable CLI (Command Line Interface) wrapped in a chat bubble, ensuring you have full control over your infrastructure without needing a laptop.

