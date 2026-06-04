# AI Agent Code Quality Refactor Plan

**Status**: Planned  
**Priority**: High - Affects readability and maintainability across 40+ files  
**Last Updated**: June 1, 2026

## Recent Completed Work (Context)

### LLM Provider Optimizations
- ✅ Gemini 2.0 Flash Thinking: Auto-inject `thinking_config="medium"` when tools present
- ✅ Gemini 2.0 Flash Thinking: Use `response_schema` to constrain structured outputs
- ✅ Carryover architecture: Live mode (native provider handles) + Compact mode (MRU fallback)
- ✅ Gemini/GPT live carryover: Store conversation state in `ConversationHandle` for continuations
- ✅ MRU cutoff enrichment: Extract session MRU context on token/ask limit cutoffs

### Transaction Lifecycle Robustness
- ✅ Context cancellation propagation: Ask → ReAct → Tools → Transaction cleanup
- ✅ Removed problematic deferred rollback from CopilotAgent.Ask
- ✅ Service is sole owner of transaction lifecycle
- ✅ HTTP handler panic recovery ensures cleanup

### Memory Infrastructure
- ✅ MRU PUSH for Service path: `persistSessionAskOutcomeMRU` writes outcomes to session.MRU
- ✅ MRU PULL on cutoff: `collectAskOutcomeMRUContext` enriches carryover summaries
- ✅ All three loops (default, Gemini, GPT) consume MRU-enriched `req.ContextText`
- ✅ Default loop integrates with MRU despite no provider-owned carryover

## Problem Statement

### Context-Based State Passing Issues

**Current State**: 16+ typed ContextKey constants, 66+ `ctx.Value()` calls, 100+ `context.WithValue()` calls

**Problems**:
1. **Hidden Dependencies**: Function signatures don't reveal dependencies
2. **Untraceable Flow**: Must trace through multiple files to find where values originate
3. **Lost Type Safety**: Runtime type assertions, silent failures
4. **Testing Complexity**: Must construct elaborate context chains for each test

**Example**:
```go
// Current: Hidden dependencies
func (s *Service) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error)
// Hidden: executor, session_payload, provider, format, database, streamer, etc.

// Target: Explicit dependencies
type AskRequest struct {
    Query    string
    Session  *SessionPayload
    Executor ToolExecutor
    Streamer func(string, any)
    Writer   io.Writer
    Options  AskOptions
}
func (s *Service) Ask(ctx context.Context, req AskRequest) (AskResponse, error)
```

## Refactor Phases

### Phase 1: Core Request/Response Structs (HIGH PRIORITY)

**Goal**: Make Ask flow dependencies explicit

**Tasks**:
- [ ] Define `AskRequest` struct with all Ask dependencies
- [ ] Define `AskResponse` struct with results + state changes
- [ ] Refactor `Service.Ask()` signature
- [ ] Refactor `resolveGeneratorAndCarryover()`, `executeReasoningEngine()`, `updateSessionMemory()`
- [ ] Update internal orchestration to pass structs

**Affected Files**: ~15 files
- `ai/agent/service.go`
- `ai/agent/carryover.go`
- `ai/agent/engine_native.go`
- Test files using Service.Ask

**Impact**: High readability improvement, affects most Ask flows

### Phase 2: Tool Execution Context (HIGH PRIORITY)

**Goal**: Make tool execution dependencies explicit

**Tasks**:
- [ ] Define `ToolExecutionContext` struct
- [ ] Refactor tool execution paths in `copilottools.go`, `copilottools.*.go`
- [ ] Update `ServiceToolExecutor` to carry context explicitly
- [ ] Remove context keys: `CtxKeyExecutor`, `CtxKeyRecorder`, `CtxKeyWriter`, `CtxKeyResultStreamer`, `CtxKeyNativeToolHints`

**Affected Files**: ~20 files
- `ai/agent/copilottools.go`
- `ai/agent/copilottools.*.go` (script, space, common, etc.)
- `ai/agent/service.runner.go`

**Impact**: High readability improvement for tool execution paths

### Phase 3: Script Orchestration (MEDIUM PRIORITY)

**Goal**: Make script orchestration state explicit

**Tasks**:
- [ ] Define `ScriptRunContext` struct
- [ ] Refactor `service.runner.go` to pass orchestration state explicitly
- [ ] Remove context keys: `CtxKeyJSONStreamer`, `CtxKeySuppressInternalStepStart`, `"step_index"`, `"verbose"`

**Affected Files**: ~10 files
- `ai/agent/service.runner.go`
- `ai/agent/service.script.go`
- `ai/agent/atomic_engine.go`

**Impact**: Medium readability improvement, simplifies runner logic

### Phase 4: Provider Configuration (LOW PRIORITY)

**Goal**: Make provider overrides explicit

**Tasks**:
- [ ] Define `GeneratorConfig` struct
- [ ] Refactor generator selection to use explicit config
- [ ] Remove context keys: `CtxKeyProvider`, `CtxKeyAPIKey`, `CtxKeyBaseURL`

**Affected Files**: ~5 files
- `ai/agent/service.go`
- `ai/agent/copilot.go`

**Impact**: Low readability improvement, but cleaner API

### Phase 5: Update Calling Sites (NECESSARY)

**Goal**: Update all callers to use new patterns

**Tasks**:
- [ ] Update HTTP handlers to construct request structs
- [ ] Update all test files
- [ ] Update CopilotAgent if needed
- [ ] Update any external callers

**Affected Files**: ~20+ files
- HTTP handler files
- All test files using Ask
- Integration test files

**Impact**: Necessary to complete refactor

### Phase 6: Cleanup and Validation (FINAL)

**Goal**: Verify correctness and clean up

**Tasks**:
- [ ] Remove unused context key constants
- [ ] Run full test suite: `go test ./...`
- [ ] Run integration tests
- [ ] Document new patterns in ARCHITECTURE.md
- [ ] Update inline documentation

**Affected Files**: All touched files
**Impact**: Ensures correctness, documents new patterns

## Context Keys to Eliminate

### High Priority (Phase 1-2)
- `CtxKeyExecutor` → Pass in ToolExecutionContext
- `CtxKeyScriptRecorder` → Pass in ToolExecutionContext
- `CtxKeyWriter` → Pass in ToolExecutionContext or AskRequest
- `CtxKeyResultStreamer` → Pass in ToolExecutionContext
- `CtxKeyNativeToolHints` → Pass in ToolExecutionContext
- `"session_payload"` → Pass in AskRequest.Session

### Medium Priority (Phase 3)
- `CtxKeyJSONStreamer` → Pass in ScriptRunContext
- `CtxKeySuppressInternalStepStart` → Pass in ScriptRunContext
- `CtxKeyCurrentScriptCategory` → Pass in ScriptRunContext
- `"step_index"` → Pass in ScriptRunContext
- `"verbose"` → Pass in ScriptRunContext

### Low Priority (Phase 4)
- `CtxKeyProvider` → Pass in GeneratorConfig
- `CtxKeyAPIKey` → Pass in GeneratorConfig
- `CtxKeyBaseURL` → Pass in GeneratorConfig

### Keep (Proper Context Usage)
- Standard Go context for cancellation/deadlines
- Request-scoped tracing IDs (if added later)
- Rare cross-cutting authentication (if truly needed)

## Implementation Order

1. **Start**: Phase 1 (AskRequest/AskResponse) - Highest impact
2. **Next**: Phase 2 (ToolExecutionContext) - Second highest impact
3. **Then**: Phase 5 (Update callers) - Necessary for phases 1-2
4. **After**: Phase 3 (ScriptRunContext) - Medium priority
5. **After**: Phase 4 (GeneratorConfig) - Low priority but easy
6. **Final**: Phase 6 (Cleanup/validation) - Always last

## Success Criteria

- ✅ Function signatures reveal all dependencies
- ✅ Parameter chains show data flow through call stack
- ✅ Compile-time validation of dependencies (no more `ctx.Value()` type assertions)
- ✅ Tests construct simple structs instead of context chains
- ✅ IDE autocomplete/go-to-definition work correctly
- ✅ All existing tests pass
- ✅ Code coverage maintained or improved

## Notes

- **Service path first**: Simpler than CopilotAgent, learn patterns here
- **Incremental**: Each phase should compile and pass tests independently
- **Compatibility**: Consider deprecation period for external callers if needed
- **Documentation**: Update ARCHITECTURE.md with new patterns after completion
