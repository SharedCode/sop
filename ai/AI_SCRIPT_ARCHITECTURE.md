# Evolution of an AI Agent: From Chatty Assistant to Scalable Data Engine

## The Vision: AI as a Functional Runtime

When we started building the SOP AI Agent, the goal was never just to create another chatbot that summarizes text. The vision was far more ambitious: **to create a conversational interface for high-scale data management.**

We wanted an AI that could:
1.  Understand natural language intent ("Show me high-salary employees in Engineering").
2.  Translate that into precise database operations.
3.  **Execute complex workflows (Scripts)** that behave like reliable server-side functions.

The ultimate goal was to have the AI act not just as a wrapper around a database, but as a **programmable engine** where a "Script" is effectively a stored procedure that can be invoked via a REST API.

## The Paradigm: Natural Language Programming

We call this interface **Natural Language Programming**. The goal is to democratize software development by allowing typical users to author programs using plain English.

SOP functions as a compiler for this new language:
1.  **Authoring:** The user describes a workflow: "Check the inventory levels, and if any item is below 10 units, create a reorder request."
2.  **Compilation:** The Agent translates these high-level intents into **machine-executable scriptlets** (our AST).
3.  **Runtime:** These scriptlets are stored as Scripts—effectively turning English instructions into repeatable, scalable software artifacts.

This shifts the role of the AI from a passive "assistant" to an active **development platform**, where the "code" is natural language and the "binary" is the JSON-based Script definition.

## Recommended Authoring Workflow

The most reliable way to build automations in SOP is to treat script authoring as an incremental engineering workflow rather than one large generation step.

1.  **Define the atomic function**: Start with one narrow script that solves a single business rule, such as computing churn risk for one customer.
2.  **Dry-run and inspect**: Execute that script with representative inputs, inspect the trace, and correct the rule before composing anything larger.
3.  **Compose the controller**: Build the higher-level workflow as a loop or orchestrator that calls the verified atomic script instead of duplicating its logic inline.
4.  **Promote to scheduled automation**: Once the trace is correct and repeatable, save the workflow as a reusable batch job or callable script.

This pattern preserves determinism. The atomic script becomes a stable unit, the controller stays simple, and later business-rule changes happen in one place.

### Worked Example: Function Then Controller

One effective pattern is to first author a script that evaluates a single record, then compose a second script that streams over the full dataset and calls that atomic script for each item.

*   **Atomic Step**: A script like `check_churn_risk` can fetch one customer's orders, evaluate the risk rule, and return structured output.
*   **Verification Step**: Run the atomic script with a known sample input and inspect the execution trace before using it elsewhere.
*   **Controller Step**: A second script can scan all customers, call `check_churn_risk`, and branch on the result for logging, coupon generation, or downstream actions.
*   **Operational Result**: The logic stays composable, reviewable, and schedulable without forcing the model to regenerate the whole workflow on every run.

## The Hardship: Growing Pains

### 1. The "Chatty" Trap
In our initial iteration, we fell into a common trap in AI development: **anthropomorphism over structure.** We designed the backend to "talk" like a human, streaming back logs of its thoughts mixed with data.

This led to **Parsing Nightmares** (frontend using Regex to hunt for JSON) and **Scalability Bottlenecks** (buffering massive responses in memory to validate them). We realized that to scale, we had to stop treating the AI as a *chat partner* and start treating it as a *compute engine*.

### 2. The State Management Nightmare
Perhaps the most difficult challenge was **Recording vs. Runtime**.
*   **Recording:** When a user says "Start recording", every subsequent action needs to be captured. But users make mistakes. They run queries that fail. They ask clarifying questions. How do you distinguish between "noise" and "intent"?
*   **Runtime:** When replaying that script, the environment is different. The transaction context is different. The variables are different.

We initially tried to share state between the "User Session" and the "Script Runner". This was a disaster. Scripts would accidentally commit user transactions, or user queries would bleed into script execution scopes. We needed a way to guarantee **stability** for the end-user recording session while maintaining a pristine environment for the runtime.

## The Architecture: Built for Scale

We completely refactored the engine around three core pillars: **AST Composability**, **Session Isolation**, and **Structured Streaming**.

### 1. The AST & Composability
We moved away from "script recording" (saving text commands) to an **Abstract Syntax Tree (AST)** approach. We defined a rigid schema for a `ScriptStep`:

```go
type ScriptStep struct {
    Name        string         // Unique identifier for the step
    Description string         // Human-readable explanation of what this step does
    Type        string         // "command", "ask", "if", "loop", "script", "tool"
    Command     string         // The actual instruction
    Args        map[string]any // Parameters
    Steps       []ScriptStep    // Nested steps (for loops/conditionals)
}
```

This design unlocked **Composability** and **Self-Documentation**.
*   **Composability**: Because a `ScriptStep` can be of type `script`, one script can call another.
*   **Self-Documentation**: The `Description` field allows the script to be read and understood by humans and LLMs alike, facilitating "Code Review" for AI agents.
*   We can build small, atomic scripts (`find_user`, `calculate_tax`).
*   We can compose them into complex workflows (`process_payroll` calls `find_user` then `calculate_tax`).
*   The runner (`runStepScript`) simply pushes a new stack frame and executes the child script, just like a function call in a programming language.

### 2. Session Isolation (Refactored)
To solve the state management nightmare, we strictly separated the **Recording Context** from the **Runtime Context**.

*   **ScriptExecutor (The Instance):** We moved away from shared `Context` maps to a robust `ScriptExecutor` struct. This instance holds:
    *   **Variables (Mutex-protected):** Safe for parallel step execution.
    *   **Transactions:** Scoped strictly to the script instance.
    *   **Dependencies:** Injected at creation time.
*   **Context Injection:** The `ScriptExecutor` is injected into the Go Context during execution, allowing generic tool signatures to retrieve their specific runtime instance safely.
*   **Recording vs. Runtime:** The Recorder captures the *intent* (AST), while the Executor manages the *state* (Variables/TX). They never share memory directly.

### 3. Structured Streaming (The Heart & Soul)
Finally, to solve the "Chatty" trap and enable scaling, we implemented the **JSON Streaming** pattern, the heart & soul of SOP's large data chunking extended to the AI space.

Instead of writing raw strings, the engine emits `StepExecutionResult` objects. We implemented a `JSONStreamer` that wraps the HTTP response writer.

```go
type StepExecutionResult struct {
    Type    string `json:"type"`    // "command", "ask", "error"
    Result  string `json:"result"`  // The raw data payload
}
```

As soon as a step finishes, it is serialized and flushed.
*   **Low Latency:** The client sees progress immediately.
*   **Low Memory:** We process 100k records, stream the result, and forget it. No massive buffers.
*   **Frontend Decoupling:** The backend sends a "Script Trace" (JSON array). The frontend decides how to render it—as a Chat bubble or a CSV table.

## The Result: A RESTful Experience

The transformation is profound. Running a complex AI script now feels exactly like calling a standard REST API endpoint.

*   **Input:** `/play script=audit_salary`
*   **Output:** A clean stream of JSON objects.
*   **Behavior:** Deterministic, machine-readable, and pipeable.

We successfully bridged the gap between the flexibility of Generative AI and the rigidity required for data engineering. The SOP Agent is no longer just "chatting" about data; it is **streaming** it.

---

## API Architecture: Dual-Layer Design

SOP provides two complementary APIs serving different domains:

### 1. Spaces API (Domain: AI Memory)
**Purpose**: High-level AI memory operations  
**File**: `ai/agent/copilottools.space.go` (585 lines, 14 functions)  
**Tools**:
- Space management: `mint_to_space`, `delete_space`, `enrich_space`
- Vectorization: `vectorize_space`, `vectorize_space_categories`, `vectorize_space_items`
- Configuration: `update_space_config`, `read_space_config`
- Category/Item CRUD: upsert, delete, list operations

**Architecture**: 
- Uses `map[string]any` with inline JSON schemas
- Domain-specific for Knowledge Bases (VectorDB, embeddings, semantic search)
- Operates on special schema (Categories/Items)

**Key Rule**: When working with Spaces, **DO NOT USE RAW DATABASE TOOLS**. Spaces are AI memory, not raw B-Trees.

### 2. Typed Database API (Domain: Data Operations)
**Purpose**: Low-level database operations with bulk scalability  
**Files**: `ai/agent/api_*.go` (4 files: types, core, bulk, transaction)  
**Operations**:
- Single ops: `Add`, `Update`, `Delete`, `Select`, `ExecuteScript`, `Join`
- Bulk ops: `BulkAdd`, `BulkUpdate`, `BulkDelete` (with 3 transaction modes)
- Transactions: `BeginTransaction`, `CommitTransaction`, `RollbackTransaction`

**Architecture**:
- Strongly typed Go structs with JSON tags
- OpenAPI schema generation from types
- Three transaction modes: `auto_batch` (scalable), `single` (atomic), `explicit` (multi-op)

**Key Benefits**:
- **Scalability**: Bulk operations handle 10K+ items efficiently
- **Type Safety**: Compile-time validation
- **Schema-Driven**: Single source of truth (Go structs → OpenAPI → LLM guidance)
- **Testability**: Direct programmatic access for test harnesses

### The Integration Pattern

```
┌─────────────────────────────────────────────────┐
│           Business Logic Layer                  │
│  ┌──────────────────┐  ┌──────────────────┐   │
│  │ Spaces API       │  │ Database API     │   │
│  │ (AI Memory)      │  │ (B-Trees)        │   │
│  └────────┬─────────┘  └────────┬─────────┘   │
└───────────┼────────────────────┼───────────────┘
            │                    │
    ┌───────┴────────┐   ┌──────┴──────┬──────────┐
    │                │   │             │          │
┌───▼────┐  ┌───────▼───▼──┐  ┌──────▼───┐  ┌───▼─────┐
│ LLM    │  │  HTTP/OpenAPI │  │ Test     │  │ Scripts │
│ Tools  │  │  Endpoints    │  │ Harness  │  │ Playback│
└────────┘  └───────────────┘  └──────────┘  └─────────┘
```

**LLM Integration**: Both APIs expose tools via map[string]any adapters. LLMs use JSON, adapters convert to typed APIs internally.

**OpenAPI Schemas**: The Database API generates OpenAPI specs from Go structs, providing concise schema references instead of verbose inline definitions (93% reduction in guidance size).

**Use Case Separation**:
- User says "Create a Notes space" → **Spaces API**
- User says "Bulk insert 50,000 records" → **Database API**
- User says "Enrich my knowledge base" → **Spaces API**
- Script needs atomic multi-operation tx → **Database API** (explicit mode)

See [ai/agent/README_API.md](agent/README_API.md) for Database API documentation.

## LLM Provider Requirements

The SOP AI Agent architecture requires specific capabilities from LLM providers to function correctly. These requirements stem from the ReAct (Reasoning + Acting) pattern and the need for reliable, structured tool execution.

### Core Architecture Dependencies

1. **Tool Calling (Function Calling)** - **BLOCKING REQUIREMENT**
   - The agent architecture depends on native tool calling support
   - LLM must accept tool definitions with JSON schemas
   - LLM must return structured ToolCall objects (not text descriptions)
   - Without this: Scripts cannot be generated, queries cannot be executed, agent is non-functional

2. **System Prompts** - **REQUIRED**
   - Must support system-level instructions independent of user messages
   - Critical for role boundaries, safety constraints, and behavioral policies
   - Used to inject: Database schema context, available tools, execution mode rules

3. **Multi-Turn Tool Continuations** - **REQUIRED**
   - Must preserve tool_use → tool_result pairs across conversation turns
   - Enables the ReAct loop to iterate without losing context
   - Without this: Agent cannot recover from errors, cannot execute multi-step workflows

### Provider Implementation Matrix

| Provider | Tool Calling | System Prompts | Multi-Turn | Native Loop | Implementation |
|----------|--------------|----------------|------------|-------------|----------------|
| **Gemini** | ✅ Native | ✅ | ✅ ToolCallContinuations | ✅ Provider-owned | `ai/generator/gemini.go` |
| **ChatGPT** | ✅ Native | ✅ | ✅ Responses API threading | ✅ Provider-owned | `ai/generator/chatgpt_react_loop.go` |
| **Claude** | ✅ Native | ✅ | ✅ Message history | ⚠️ Generic path | `ai/generator/anthropic.go` |
| **Ollama** | ❌ Not supported | ✅ | ❌ No tool support | N/A | `ai/generator/ollama.go` (Embeddings only) |

### Carryover Implementation Details

The agent uses a **two-level carryover architecture** to maintain conversation state:

#### Macro-Level: Inter-Ask Carryover (`ai/agent/carryover.go`)
- Decides whether to continue conversation or reset
- Budget limits: `AskCount`, `EstimatedCarryTokens`, `EstimatedRawToolTokens`
- When budget exceeded: Falls back to **compact mode** with MRU enrichment
- MRU context includes: Store schemas, relations, join/filter selections, confirmed facts, tool patterns

#### Micro-Level: Intra-ReAct Loop (Provider-Specific)
- **Gemini**: `ToolCallContinuations` array in `GenOptions`
  - Preserves full function call/response state
  - Native Gemini format: `functionCall`/`functionResponse` objects
- **ChatGPT**: `PreviousResponseID` via OpenAI Responses API
  - Provider-native thread continuation
  - Opaque conversation handle preserved across turns
- **Claude**: Message history with `tool_use`/`tool_result` content blocks
  - Anthropic native format: Array of messages with alternating assistant/user roles
  - Each tool call becomes `tool_use` block, result becomes `tool_result` block

### Design Principle: Explicit Parameters

**Critical**: All provider implementations follow the **explicit parameter design**:
- Tool definitions passed via `GenOptions.Tools` (NOT extracted from Context)
- Tool continuations passed via `GenOptions.ToolCallContinuations` (NOT from Context)
- System prompts passed via `GenOptions.SystemPrompt` (NOT from Context)
- Context only used for: Cancellation signals, tracing metadata

This ensures:
- Clear dependency boundaries
- Testable without complex context mocking
- Provider implementations remain stateless

### Model Selection Criteria

When adding models to `model_catalog.json`:

**MUST HAVE:**
- ✅ Tool calling with JSON schema support
- ✅ System prompt support
- ✅ Multi-turn conversation with tool result continuations

**NICE TO HAVE:**
- ⭐ Thinking/reasoning controls (Gemini-specific)
- ⭐ Structured output schema enforcement (Gemini-specific)
- ⭐ Provider-owned loop support (better error recovery)

**EXCLUDE:**
- ❌ Models without function calling support
- ❌ Chat-only models (no tool execution)
- ❌ Preview/experimental models not production-ready
- ❌ Models with unreliable schema parsing

### Testing Contract

All providers must pass the contract tests in `ai/generator/provider_event_contract_test.go`:
- Parse tool calls from responses correctly
- Emit `ReasoningEventToolCall` events with proper structure
- Include `tool`, `args`, and `iteration` in event payloads
- Handle tool result feedback and continue reasoning loop

See provider implementations for reference patterns.
