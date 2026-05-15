package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/obfuscation"
)

// TextReActEngine implements ReasoningEngine using the legacy Markdown JSON parsing logic.
type TextReActEngine struct {
	EnableObfuscation bool
}

// Run executes the legacy orchestration loop.
func (e *TextReActEngine) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	if req.Generator == nil {
		if e.EnableObfuscation {
			return ai.ReasoningResponse{FinalText: obfuscation.GlobalObfuscator.DeobfuscateText(req.ContextText)}, nil
		}
		return ai.ReasoningResponse{FinalText: req.ContextText}, nil
	}

	fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s%s\n\nUser Query: %s", req.SystemPrompt, req.ContextText, req.HistoryText, req.UserQuery)

	output, err := req.Generator.Generate(ctx, fullPrompt, ai.GenOptions{
		MaxTokens:   1024,
		Temperature: 0.7,
	})
	if err != nil {
		return ai.ReasoningResponse{}, fmt.Errorf("generation failed: %w", err)
	}

	if req.Executor != nil {
		text := strings.TrimSpace(output.Text)
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)

		possibleTool := false
		if strings.HasPrefix(text, "{") && (strings.Contains(text, "\"tool\"") || strings.Contains(text, "\"tool_calls\"")) {
			possibleTool = true
		}

		if !possibleTool {
			start := strings.Index(output.Text, "```json")
			if start != -1 {
				sub := output.Text[start+7:]
				end := strings.Index(sub, "```")
				if end != -1 {
					jsonText := strings.TrimSpace(sub[:end])
					if strings.HasPrefix(jsonText, "{") && (strings.Contains(jsonText, "\"tool\"") || strings.Contains(jsonText, "\"tool_calls\"")) {
						possibleTool = true
						text = jsonText
					}
				}
			}
		}

		if possibleTool {
			var toolCall struct {
				Tool      string         `json:"tool"`
				Args      map[string]any `json:"args"`
				Arguments map[string]any `json:"arguments"`
			}
			var openaiCall struct {
				ToolCalls []struct {
					Name      string         `json:"name"`
					Arguments map[string]any `json:"arguments"`
				} `json:"tool_calls"`
			}

			unmarshaled := false
			if err := json.Unmarshal([]byte(text), &toolCall); err == nil && toolCall.Tool != "" {
				if toolCall.Args == nil && toolCall.Arguments != nil {
					toolCall.Args = toolCall.Arguments
				}
				unmarshaled = true
			} else if err := json.Unmarshal([]byte(text), &openaiCall); err == nil && len(openaiCall.ToolCalls) > 0 {
				toolCall.Tool = openaiCall.ToolCalls[0].Name
				toolCall.Args = openaiCall.ToolCalls[0].Arguments
				unmarshaled = true
			}

			if unmarshaled {
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

				result, err := req.Executor.Execute(ctx, toolCall.Tool, toolCall.Args)
				if err != nil {
					return ai.ReasoningResponse{}, fmt.Errorf("tool execution failed: %w", err)
				}

				return ai.ReasoningResponse{
					FinalText: result,
					ToolCalls: []ai.ToolCall{{Name: toolCall.Tool, Args: toolCall.Args}},
				}, nil
			}
		}
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
