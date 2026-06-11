# Standalone Tool Result Consumer Design

This is an isolated design artifact for a shared tool-result processing boundary.
It is intentionally not wired into the current runtime.

## Goal

Create one reusable result-processing function that:

- traverses a tool result once
- can stream normalized items to the UI
- can build bounded LLM-facing continuation payloads
- can preserve raw structured output when needed
- can be extended later with Ask/MRU/STM/LTM-aware analysis without coupling that into this phase

The design is meant to replace ad hoc string serializers and one-off reducers with a single, policy-driven boundary.

## Design Rules

1. The shared boundary must run at the cursor-drain / result-traversal path.
2. UI streaming and LLM continuation shaping are different consumers of the same traversal.
3. Full JSON materialization must be optional, not assumed.
4. Business logic for "managing the LLM" belongs in policy and consumer selection, not in tool handlers.
5. The shared boundary should return structured outcome, not only a string.

## Core Types

```go
package agent

import "context"

type ToolResultOutcome struct {
    RawResult string
    LLMResult string
    Facts     []string
    Stats     ToolResultStats
    Meta      map[string]any
}

type ToolResultStats struct {
    Shape       string
    RowCount    int64
    SampleCount int
    Truncated   bool
    ApproxBytes int64
}

type ToolResultPolicy struct {
    ToolName             string
    IncludeRawResult     bool
    IncludeLLMResult     bool
    WrapNativeEnvelope   bool
    MaxPreviewRows       int
    MaxPreviewBytes      int
    ImportantFields      []string
    IntermediateStyle    string
    FinalStyle           string
    NewLLMConsumer       func(ToolResultPolicy, ToolResultContext) ResultConsumer
    NewRawConsumer       func(ToolResultPolicy, ToolResultContext) ResultConsumer
    NewUIConsumer        func(ToolResultPolicy, ToolResultContext) ResultConsumer
}

type ToolResultContext struct {
    ToolName          string
    NativeToolHints   bool
    FinalToolResult   bool
    AskPrompt         string
    ToolCallIndex     int
    SessionFacts      []string
}

type ResultConsumer interface {
    ObserveRow(item any) error
    ObserveScalar(value any) error
    ObserveText(text string) error
    Finish() (ToolResultOutcome, error)
}
```

## Recommended Consumers

```go
type CompositeResultConsumer struct {
    consumers []ResultConsumer
}

type RawResultConsumer struct {
    // preserves raw structured output when policy asks for it
}

type UIResultConsumer struct {
    // forwards items to the existing JSON/UI streamer
}

type LLMResultConsumer struct {
    // keeps bounded state for continuation payloads
}
```

### Notes

- `CompositeResultConsumer` fans out one traversal to multiple consumers.
- `RawResultConsumer` is the compatibility surface for callers that still need raw JSON/text.
- `UIResultConsumer` is an adapter over the current UI streaming surface.
- `LLMResultConsumer` is where reducer logic lives.

## Main Entry Point

This should be the dominant public boundary:

```go
func processToolResult(ctx context.Context, toolName string, value any, input ToolResultContext) (ToolResultOutcome, error)
```

Suggested implementation shape:

```go
func processToolResult(ctx context.Context, toolName string, value any, input ToolResultContext) (ToolResultOutcome, error) {
    policy := resolveToolResultPolicy(toolName, input)
    consumer := buildResultConsumer(policy, input)
    return consumeToolResultValue(ctx, value, consumer)
}
```

## Policy Resolution

Policy is the business-logic seam.
This is where you decide how a tool should help the next LLM turn.

```go
func resolveToolResultPolicy(toolName string, input ToolResultContext) ToolResultPolicy
```

Typical policy decisions:

- preserve raw output for direct/manual execution
- emit bounded LLM context for native Ask-loop execution
- sample rows for `execute_script`
- keep only counts for noisy bulk tools
- preserve selected fields for schema/relation tools
- phrase final tool results differently from intermediate continuation turns

## Traversal Function

This function performs the actual one-pass traversal.
Everything else is setup and policy.

```go
func consumeToolResultValue(ctx context.Context, value any, consumer ResultConsumer) (ToolResultOutcome, error)
```

Behavior:

1. Detect cursor vs list vs scalar vs text.
2. Normalize each item once.
3. Feed normalized items to the consumer.
4. Do not materialize full JSON unless the active consumer explicitly requires it.
5. Return the consumer outcome.

## Normalization Hook

Keep row/item normalization independent from consumer logic.

```go
func normalizeToolResultItem(item any, orderedFields []string) (any, error)
```

Normalization may include:

- collapsing internal wrappers
- filtering by ordered fields
- removing internal-only structure
- preserving shape useful for both UI and LLM consumers

## Example Consumer Composition

```go
func buildResultConsumer(policy ToolResultPolicy, input ToolResultContext) ResultConsumer {
    consumers := make([]ResultConsumer, 0, 3)

    if policy.IncludeRawResult && policy.NewRawConsumer != nil {
        consumers = append(consumers, policy.NewRawConsumer(policy, input))
    }
    if policy.NewUIConsumer != nil {
        consumers = append(consumers, policy.NewUIConsumer(policy, input))
    }
    if policy.IncludeLLMResult && policy.NewLLMConsumer != nil {
        consumers = append(consumers, policy.NewLLMConsumer(policy, input))
    }

    if len(consumers) == 1 {
        return consumers[0]
    }
    return &CompositeResultConsumer{consumers: consumers}
}
```

## Example `execute_script` LLM Consumer

```go
type ExecuteScriptLLMConsumer struct {
    rowCount  int64
    sample    []any
    text      string
    scalar    any
    truncated bool
    policy    ToolResultPolicy
}

func (c *ExecuteScriptLLMConsumer) ObserveRow(item any) error {
    c.rowCount++
    if len(c.sample) < c.policy.MaxPreviewRows {
        c.sample = append(c.sample, item)
    } else {
        c.truncated = true
    }
    return nil
}

func (c *ExecuteScriptLLMConsumer) ObserveScalar(value any) error {
    if c.rowCount > 0 || c.text != "" || c.scalar != nil {
        return nil
    }
    c.scalar = value
    return nil
}

func (c *ExecuteScriptLLMConsumer) ObserveText(text string) error {
    if c.rowCount > 0 || c.scalar != nil || c.text != "" {
        return nil
    }
    c.text = text
    if len(text) > c.policy.MaxPreviewBytes {
        c.truncated = true
    }
    return nil
}

func (c *ExecuteScriptLLMConsumer) Finish() (ToolResultOutcome, error) {
    outcome := ToolResultOutcome{
        Stats: ToolResultStats{
            Shape:       detectShape(c),
            RowCount:    c.rowCount,
            SampleCount: len(c.sample),
            Truncated:   c.truncated,
        },
    }

    if c.rowCount > 0 {
        outcome.LLMResult = formatExecuteScriptRowSummary(c.rowCount, c.sample, c.truncated)
        return outcome, nil
    }
    if c.scalar != nil {
        outcome.LLMResult = formatExecuteScriptScalarSummary(c.scalar, c.policy.MaxPreviewBytes)
        return outcome, nil
    }
    outcome.LLMResult = formatExecuteScriptTextSummary(c.text, c.policy.MaxPreviewBytes)
    return outcome, nil
}
```

## Compatibility Adapters

Old string-returning paths should become thin wrappers over `processToolResult`, not the other way around.

```go
func serializeToolResult(ctx context.Context, toolName string, value any, input ToolResultContext) (string, error) {
    outcome, err := processToolResult(ctx, toolName, value, input)
    if err != nil {
        return "", err
    }
    if outcome.LLMResult != "" {
        return outcome.LLMResult, nil
    }
    return outcome.RawResult, nil
}
```

If native tool envelopes are still required:

```go
func wrapToolResultForNativeLoop(outcome ToolResultOutcome, policy ToolResultPolicy) (string, error)
```

## Future-Compatible Ask Analysis

This phase should not wire MRU/STM/LTM mutations directly into `processToolResult`.
But the output should be ready for later use by Ask-level analysis.

That is why `ToolResultContext` and `ToolResultOutcome` should remain open to:

- initial Ask prompt
- tool-call sequence metadata
- MRU facts
- STM/LTM retrieval results
- richer `Facts` and `Meta`

This keeps the result boundary complementary with the broader analytical machine without entangling everything now.

## Recommended First Integration Order

1. Add these types and functions standalone.
2. Make `processToolResult(...)` the main path.
3. Keep old string helpers as wrappers only.
4. Start with one real LLM policy: `execute_script`.
5. Add more policies after the main boundary is stable.

## Handoff Summary

The intended architecture is:

- one traversal
- one policy resolver
- multiple consumers
- one structured outcome
- optional compatibility wrappers

That is the clean reusable seam for both current bounded continuation payloads and future Ask-analysis enrichment.
