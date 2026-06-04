# Gemini 3.1 Pro Optimizations Integration

## Overview

Integrated two key Gemini 3.1 Pro optimizations to improve tool calling reliability, structured output adherence, and performance:

1. **ThinkingConfig** - Controls internal reasoning intensity
2. **ResponseSchema** - Hard-constrains output structure

## Implementation

### Core Changes

#### 1. `ai/interfaces.go` - GenOptions Extended
```go
type GenOptions struct {
    // ... existing fields ...
    
    // ThinkingLevel controls internal reasoning intensity for Gemini 3.1 Pro (low/medium/high).
    // Use "low" for strict structured outputs, "high" for creative tasks.
    ThinkingLevel string
    
    // ResponseSchema enforces strict output structure by physically constraining token generation.
    // Pass a JSON schema to prevent hallucinated fields/keys.
    ResponseSchema map[string]any
}
```

#### 2. `ai/generator/gemini.go` - Request Builder Enhanced
```go
type geminiGenerationConfig struct {
    Temperature     float32              `json:"temperature,omitempty"`
    TopP            float32              `json:"topP,omitempty"`
    MaxOutputTokens int                  `json:"maxOutputTokens,omitempty"`
    ThinkingConfig  *geminiThinkingConfig `json:"thinkingConfig,omitempty"`  // NEW
    ResponseSchema  map[string]any       `json:"responseSchema,omitempty"`  // NEW
}

type geminiThinkingConfig struct {
    ThinkingLevel string `json:"thinkingLevel,omitempty"`
}
```

**Auto-Detection Logic:**
- When `Tools` are present but no explicit `ThinkingLevel` is set → automatically uses `"medium"`
- Prevents over-analysis and creative syntax variations in tool schemas
- Uses existing `sanitizeGeminiSchema` helper for ResponseSchema validation

### Integration Points

#### 3. `ai/agent/engine_native.go` - ReAct Loop Optimization
- **Tool iterations**: `ThinkingLevel = "low"` (strict schema adherence)
- **Repair attempts**: `ThinkingLevel = "medium"` (more reasoning for strategies)
- **Final synthesis**: `ThinkingLevel = "high"` (creative explanations)

#### 4. `ai/agent/classifier.go` - Structured Classification
All three classifiers now use `ThinkingLevel = "low"`:
- `ClassifyDiscoveryTaskContext`
- `ClassifyFocusedTaskContext`
- `ClassifyContinuityTaskContext`

**Benefit:** Strict JSON schema adherence, no creative field variations

#### 5. `ai/memory/manager.go` - Memory Operations
All LLM-based memory operations use `ThinkingLevel = "low"`:
- `GenerateCategory` - Single thought categorization
- `GenerateCategories` - Batch categorization
- `GenerateTaxonomy` - Structured fact extraction
- `GenerateSummaries` - Summary generation

**Benefit:** Consistent formatting (comma/pipe-separated outputs)

#### 6. `ai/agent/service.go` - Topic Routing
Topic routing classification uses `ThinkingLevel = "low"`

**Benefit:** Deterministic JSON output for topic/thread management

#### 7. `ai/agent/service.script.go` - Script Refinement
Script template refinement uses `ThinkingLevel = "low"`

**Benefit:** Precise JSON transformations, no creative parameter additions

## Usage Examples

### Explicit Control
```go
opts := ai.GenOptions{
    ThinkingLevel: "low",  // Strict schema adherence
    ResponseSchema: map[string]any{
        "type": "object",
        "properties": map[string]any{
            "answer": map[string]any{"type": "string"},
        },
    },
}
```

### Auto-Detection (Tools)
```go
opts := ai.GenOptions{
    Tools: []ai.ToolDefinition{
        {Name: "query_store", Description: "Query data"},
    },
    // ThinkingLevel automatically set to "medium"
}
```

## Benefits

1. **Performance** - Lower thinking levels reduce internal reasoning overhead
2. **Reliability** - Prevents creative schema variations in tool calls
3. **Consistency** - Hard-constrains structured outputs via ResponseSchema
4. **Backward Compatible** - Works with gemini-3.5-flash, optimized for gemini-3.1-pro

## Testing

Run the integration demo:
```bash
go run examples/demo_gemini_optimizations.go
```

## Model Compatibility

- **gemini-3.5-flash**: Parameters accepted but may not use internal thinking
- **gemini-3.1-pro**: Full optimization support
- **gemini-2.0-flash-exp**: Full optimization support
- Future models: Forward compatible via v1beta API

## References

- Blog post optimizations: thinking_config and response_schema
- Gemini API v1beta endpoint: `https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent`
