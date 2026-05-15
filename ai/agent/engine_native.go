package agent

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/obfuscation"
)

// NativeReActEngine implements ReasoningEngine using native LLM API tool calling.
type NativeReActEngine struct {
	EnableObfuscation bool
}

// Run executes the orchestration loop relying on native tool calls.
func (e *NativeReActEngine) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	if req.Generator == nil {
		if e.EnableObfuscation {
			return ai.ReasoningResponse{FinalText: obfuscation.GlobalObfuscator.DeobfuscateText(req.ContextText)}, nil
		}
		return ai.ReasoningResponse{FinalText: req.ContextText}, nil
	}

	var tools []ai.ToolDefinition
	var err error
	if req.Executor != nil {
		tools, err = req.Executor.ListTools(ctx)
		if err != nil {
			return ai.ReasoningResponse{}, fmt.Errorf("failed to list tools: %w", err)
		}
	}

	mainPrompt := fmt.Sprintf("Context:\n%s%s\n\nUser Query: %s", req.ContextText, req.HistoryText, req.UserQuery)

	output, err := req.Generator.Generate(ctx, mainPrompt, ai.GenOptions{
		SystemPrompt: req.SystemPrompt,
		MaxTokens:    1024,
		Temperature:  0.7,
		Tools:        tools,
	})
	if err != nil {
		return ai.ReasoningResponse{}, fmt.Errorf("generation failed: %w", err)
	}

	if req.Executor != nil && len(output.ToolCalls) > 0 {
		toolCall := output.ToolCalls[0]

		var sanitize func(any) any
		sanitize = func(v any) any {
			switch val := v.(type) {
			case string:
				val = strings.Trim(val, "*_`")
				val = strings.ReplaceAll(val, "\u00a0", " ")
				val = strings.TrimSpace(val)
				if e.EnableObfuscation {
					val = obfuscation.GlobalObfuscator.DeobfuscateText(val)
				}
				return val
			case []any:
				for i, item := range val {
					val[i] = sanitize(item)
				}
				return val
			case map[string]any:
				for k, item := range val {
					val[k] = sanitize(item)
				}
				return val
			default:
				return val
			}
		}

		for k, v := range toolCall.Args {
			toolCall.Args[k] = sanitize(v)
		}

		result, err := req.Executor.Execute(ctx, toolCall.Name, toolCall.Args)
		if err != nil {
			return ai.ReasoningResponse{}, fmt.Errorf("tool execution failed: %w", err)
		}

		return ai.ReasoningResponse{
			FinalText: result,
			ToolCalls: []ai.ToolCall{toolCall},
		}, nil
	}

	finalText := output.Text
	if e.EnableObfuscation {
		finalText = obfuscation.GlobalObfuscator.DeobfuscateText(output.Text)
	}

	return ai.ReasoningResponse{
		FinalText: finalText,
		ToolCalls: nil,
	}, nil
}
