# Recent LLM Agent Progression (May-June 2026)

**Period**: May 2026 - June 1, 2026  
**Focus**: Provider optimizations, transaction robustness, memory infrastructure

---

## 1. Gemini 2.0 Flash Thinking Optimizations

### Thinking Config
**Problem**: Gemini 2.0 Flash Thinking models require explicit `thinking_config` parameter to enable structured reasoning before tool selection.

**Solution**: Auto-detection in `ai/generator/gemini.go`
```go
// Auto-inject "medium" thinking level when tools are present
if len(opts.Tools) > 0 && opts.ThinkingLevel == "" {
    opts.ThinkingLevel = "medium"
}
```

**Impact**: LLM performs better tool selection without manual prompt engineering

### Response Schema
**Problem**: Gemini models can hallucinate fields in structured outputs without strict schema enforcement.

**Solution**: Use `response_schema` parameter to physically constrain token generation
```go
req.GenerationConfig.ResponseSchema = opts.ResponseSchema
```

**Impact**: Prevents invalid tool call arguments, reduces retry loops

**Files Changed**:
- `ai/generator/gemini.go`
- `ai/agent/engine_native.go`
- `ai/agent/classifier.go`

---

## 2. Transaction Lifecycle Robustness

### Context Cancellation Propagation
**Problem**: When users aborted in-flight Ask requests, context cancellation wasn't propagating to transactions, leaving orphaned locks.

**Solution**: Full cancellation chain
```
HTTP Handler → Service.Ask → executeReasoningEngine → Tool Execution → Transaction.Commit/Rollback
```

**Implementation**:
- Check `ctx.Err()` at transaction boundaries
- Rollback on cancellation before returning
- HTTP handler panic recovery ensures cleanup

### Removed Problematic Defer
**Problem**: `CopilotAgent.Ask` had `defer tx.Rollback()` that ALWAYS ran, leaving locks unreleased.

**Solution**: Removed defer, made Service the sole transaction owner

**Impact**: No more orphaned locks, cleaner ownership model

### Double-Close Coordination
**Problem**: Both `CopilotAgent.Close()` and `Service.Close()` tried to manage same transaction.

**Solution**: Removed `p.Close(ctx)` from CopilotAgent - Service owns lifecycle

**Files Changed**:
- `ai/agent/service.go`
- `ai/agent/copilot.go`
- `ai/agent/service.session.go`

---

## 3. Carryover Architecture for Efficient Context Management

### Two-Mode System

**Compact Mode** (Fallback):
- Used when provider doesn't support native continuations or limits exceeded
- Builds MRU-enriched summary text from previous Ask outcomes
- Injected as plain text context for next Ask

**Live Mode** (Provider-Native):
- Used by Gemini and ChatGPT when supported
- Stores native conversation handle (Gemini contents array, OpenAI response ID)
- Enables provider to continue conversation without flattening to text

### Carryover Decision Logic (`ai/agent/carryover.go`)

Detects cutoff scenarios:
- Token budget exceeded (8K soft, 12K hard limits)
- Ask count exceeded (4 asks in live mode)
- Topic switch detected
- Provider/model changed

Chooses mode:
- Live if supported and under limits
- Compact otherwise, enriched with MRU context

### Gemini Live Carryover

**Implementation**: Store `contents` array in `CarryoverState.ConversationHandle` as JSON
```go
func geminiOwnedLoopCarryoverState(continuations []genai.Content) *CarryoverState {
    handle, _ := json.Marshal(continuations)
    return &CarryoverState{
        Mode: ai.CarryoverModeLive,
        ConversationHandle: string(handle),
    }
}
```

**Restoration**:
```go
json.Unmarshal([]byte(req.CarryoverState.ConversationHandle), &continuations)
```

### ChatGPT Live Carryover

**Implementation**: Store `PreviousResponseID` in `ConversationHandle`
```go
func buildChatGPTResponsesRequest(req ai.ReasoningRequest) openAIResponsesRequest {
    if req.CarryoverState != nil && req.CarryoverState.Mode == ai.CarryoverModeLive {
        request.PreviousResponseID = req.CarryoverState.ConversationHandle
    }
}
```

**Files Changed**:
- `ai/interfaces.go` - CarryoverCapability, CarryoverState types
- `ai/agent/carryover.go` - Decision logic, cutoff detection
- `ai/generator/gemini.go` - Live carryover implementation
- `ai/generator/chatgpt_react_loop.go` - Live carryover implementation

---

## 4. MRU Infrastructure for Session Memory

### Problem
Service path (Paths B & C for Gemini/GPT) had no MRU PUSH after Ask completion. Only CopilotAgent (Path A) had always-on MRU hydration.

### Solution: Unified MRU Infrastructure

**MRU PUSH** (After Ask Completes):
```go
func (s *Service) updateSessionMemory(...) {
    // Line 1257: Calls MRU PUSH
    persistSessionAskOutcomeMRU(ctx, s.session, query, finalText, toolCalls, outcomeFacts)
}
```

**MRU PULL** (On Cutoff):
```go
func collectAskOutcomeMRUContext(session *RunnerSession) string {
    // Lines 213-273: Extract session MRU items
    // Categories: Database, Domain, Query, StoreSchema, Relations, JoinSelection, FilterSelection
}

func decisionWithFallback(decision, state, session) {
    // Line 275: Enrich cutoff summary with MRU context
    mruContext := collectAskOutcomeMRUContext(session)
    decision.Summary = decision.Summary + "\n\n" + mruContext
}
```

**MRU Injection** (Into All Loops):
```go
// service.go:1178 - executeReasoningEngine
if strings.TrimSpace(genCfg.carryover.Summary) != "" {
    contextText = appendCarryoverToContext(contextText, genCfg.carryover.Summary)
}
req := ai.ReasoningRequest{
    ContextText: contextText,  // Contains MRU-enriched carryover
}
```

**All Three Loops Consume**:
- Default loop: `engine_native.go:664` - `req.ContextText` in prompt sections
- Gemini loop: `gemini.go:390` - `req.ContextText` in prompt parts
- GPT loop: `chatgpt_react_loop.go:240` - `req.ContextText` in prompt parts

### Default Loop Integration

**Key Insight**: Default loop has no provider-owned carryover (no ConversationHandle, no PreviousResponseID) but still benefits from:
1. **MRU PULL on cutoff**: Gets enriched context via `req.ContextText` from Compact mode
2. **MRU PUSH after completion**: Outcomes written to session.MRU for next Ask

**Flow**:
```
Service.Ask() 
  → resolveGeneratorAndCarryover() 
    → decideCarryover() detects cutoff
      → decisionWithFallback() enriches with MRU
        → collectAskOutcomeMRUContext() extracts session MRU
  → executeReasoningEngine()
    → injects MRU-enriched summary into req.ContextText
      → NativeReActEngine.runDefaultLoop() consumes req.ContextText
  → updateSessionMemory()
    → persistSessionAskOutcomeMRU() writes to session.MRU
```

**Files Changed**:
- `ai/agent/service.go` - MRU PUSH at line 1257, infrastructure functions at lines 1374-1461
- `ai/agent/carryover.go` - MRU PULL in collectAskOutcomeMRUContext, enrichment in decisionWithFallback
- `ai/agent/memory_shortterm.go` - MRU storage in RunnerSession

---

## 5. Architecture Clarifications

### Pipeline vs Provider (Resolved Confusion)

**Pipeline** (Dormant Feature):
- Agent-level orchestration where Service chains multiple agents
- Only used in tests, not active in production
- Early return at service.go:1318 prevents overlap

**Provider** (Active Feature):
- LLM selection within single Ask (Gemini/GPT/default)
- Always runs via NativeReActEngine
- Dispatches to provider-owned loop or default loop

### Three ReAct Loops

1. **Default Loop** (`engine_native.go:119`):
   - Fallback when provider doesn't implement ReActLoopProvider
   - Shared infrastructure in NativeReActEngine
   - Used by both CopilotAgent and Service paths

2. **Gemini Loop** (`gemini.go:388`):
   - Provider-owned, implements ReActLoopProvider
   - Has live carryover with contents array

3. **ChatGPT Loop** (`chatgpt_react_loop.go:238`):
   - Provider-owned, implements ReActLoopProvider
   - Has live carryover with PreviousResponseID

### Path Separation

**Path A** (CopilotAgent - Production):
- Frozen, production path
- Has own always-on MRU hydration
- Uses shared NativeReActEngine but with custom buildSystemPrompt

**Paths B & C** (Service → Gemini/GPT - Test/Specialized):
- Simpler test/specialized path
- Now has MRU PUSH infrastructure (completed June 2026)
- Routes to NativeReActEngine which dispatches to loops

**No Dual Push**: Pipeline early return prevents overlap between paths

---

## Impact Summary

### Before
- ❌ Gemini models required manual prompt engineering for structured reasoning
- ❌ Transaction locks orphaned when requests cancelled
- ❌ Carryover used weak fallback summaries, no provider-native continuations
- ❌ Service path had no MRU persistence between Asks
- ❌ Default loop couldn't benefit from cutoff continuity

### After
- ✅ Gemini auto-detects thinking level and enforces response schemas
- ✅ Transaction cleanup on cancellation, panic, and normal exit
- ✅ Dual-mode carryover: Live (native handles) + Compact (MRU-enriched)
- ✅ All three loops (default, Gemini, GPT) get MRU PUSH/PULL
- ✅ Default loop receives rich context on cutoff via Compact mode

---

## Test Coverage

All changes validated by existing test suite:
```bash
cd /Volumes/BigDrive/sop/ai
go test ./agent/ -run "TestAsk|TestScript|TestCarryover|TestService"
# All passing as of June 1, 2026
```

---

## Next Steps

See `REFACTOR_PLAN.md` for upcoming code quality improvements:
- Move from context-based state passing to explicit parameters
- Improve readability and maintainability across 40+ files
- Phases: Request structs → Tool context → Script orchestration → Provider config
