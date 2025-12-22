package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// DataAdmin is a hybrid generator that acts as a specialized Data Administrator agent.
// It wraps an underlying LLM (Gemini, Ollama, etc.) and manages the ReAct loop
// for tool execution, effectively making it an autonomous agent within a Generator interface.
type DataAdmin struct {
	name  string
	brain ai.Generator
}

func init() {
	Register("data-admin", func(cfg map[string]any) (ai.Generator, error) {
		// Determine Provider (Logic from main.go)
		provider, _ := cfg["provider"].(string)
		if provider == "" {
			provider = os.Getenv("AI_PROVIDER")
		}

		geminiKey := strings.TrimSpace(os.Getenv("GEMINI_API_KEY"))
		openAIKey := strings.TrimSpace(os.Getenv("OPENAI_API_KEY"))

		if provider == "" {
			if openAIKey != "" {
				provider = "chatgpt"
			} else if geminiKey != "" {
				provider = "gemini"
			}
		}

		var gen ai.Generator
		var err error

		// Initialize the "Brain"
		if provider == "chatgpt" && openAIKey != "" {
			model := os.Getenv("OPENAI_MODEL")
			if model == "" {
				model = "gpt-4o"
			}
			gen, err = New("chatgpt", map[string]any{
				"api_key": openAIKey,
				"model":   model,
			})
		} else if (provider == "gemini" || provider == "local" || provider == "") && geminiKey != "" {
			// Default "local" to Gemini for now (Hybrid mode)
			model := os.Getenv("GEMINI_MODEL")
			if model == "" {
				model = "gemini-2.5-flash"
			}
			gen, err = New("gemini", map[string]any{
				"api_key": geminiKey,
				"model":   model,
			})
		} else if provider == "ollama" {
			model := os.Getenv("OLLAMA_MODEL")
			if model == "" {
				model = "llama3"
			}
			host := os.Getenv("OLLAMA_HOST")
			if host == "" {
				host = "http://localhost:11434"
			}
			gen, err = New("ollama", map[string]any{
				"base_url": host,
				"model":    model,
			})
		} else {
			return nil, fmt.Errorf("no AI Provider configured for DataAdmin. Set GEMINI_API_KEY or OPENAI_API_KEY")
		}

		if err != nil {
			return nil, fmt.Errorf("failed to initialize DataAdmin brain: %w", err)
		}

		return &DataAdmin{
			name:  "data-admin",
			brain: gen,
		}, nil
	})
}

func (g *DataAdmin) Name() string { return g.name }

func (g *DataAdmin) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	// 0. Check for Provider Override (Dynamic Switching)
	brain := g.brain
	if provider, ok := ctx.Value(ai.CtxKeyProvider).(string); ok && provider != "" {
		// If the requested provider is different from the current brain's name (if available)
		// Note: We can't easily check brain.Name() if it's not exposed, but we can try to instantiate.
		// For simplicity, if a provider is explicitly requested in context, we try to use it.
		if newBrain, err := New(provider, nil); err == nil {
			brain = newBrain
		}
	}

	// 1. Construct System Prompt with Tools
	toolsDef := `
You have access to the following tools to help answer the user's question.
To use a tool, you MUST output a JSON object in the following format ONLY, with no other text:
{"tool": "tool_name", "args": { ... }}

Tools:
1. select(database: string, store: string, limit: number, format: string) - Retrieve data.
   - 'limit': defaults to 10.
   - 'format': "csv" or "json". Default is "csv".
   - Example: {"tool": "select", "args": {"store": "users", "limit": 5}}

2. list_stores(database: string) - Lists all stores.
3. list_databases() - Lists all available databases.
4. search(database: string, store: string, query: object) - Search by field.
5. get_schema(database: string, store: string) - Get schema.

IMPORTANT:
- If user asks for "JSON", you MUST set "format": "json".
- The 'select' tool returns the raw data string. You MUST include this raw data in your final response.
`
	// We prepend the tools definition to the prompt.
	fullPrompt := toolsDef + "\n" + prompt

	// 2. ReAct Loop
	maxTurns := 5
	history := fullPrompt

	for i := 0; i < maxTurns; i++ {
		resp, err := brain.Generate(ctx, history, opts)
		if err != nil {
			return ai.GenOutput{}, err
		}

		text := strings.TrimSpace(resp.Text)

		// Check for Tool Call (Handle raw JSON or Markdown code blocks)
		isToolCall := false
		if strings.Contains(text, "\"tool\"") {
			if strings.HasPrefix(text, "{") {
				isToolCall = true
			} else if strings.HasPrefix(text, "```") {
				isToolCall = true
			}
		}

		if isToolCall {
			var toolCall struct {
				Tool string         `json:"tool"`
				Args map[string]any `json:"args"`
			}
			cleanText := strings.TrimPrefix(text, "```json")
			cleanText = strings.TrimPrefix(cleanText, "```")
			cleanText = strings.TrimSuffix(cleanText, "```")
			cleanText = strings.TrimSpace(cleanText)

			if err := json.Unmarshal([]byte(cleanText), &toolCall); err == nil && toolCall.Tool != "" {
				// Execute Tool
				executor, ok := ctx.Value(ai.CtxKeyExecutor).(ai.ToolExecutor)
				if !ok || executor == nil {
					// If no executor, we can't run the tool. Return the JSON so the caller can handle it.
					return resp, nil
				}

				// Execute
				result, err := executor.Execute(ctx, toolCall.Tool, toolCall.Args)
				if err != nil {
					result = "Error: " + err.Error()
				}

				// OPTIMIZATION: Return tool output directly without summarization (Skip Step 3)
				return ai.GenOutput{Text: result}, nil

				// Feed back to history
				// history += fmt.Sprintf("\nAssistant: %s\nSystem: Tool Output: %s\n", text, result)
				// continue
			}
		}

		// If not a tool call, or we're done, return the response
		return resp, nil
	}

	return ai.GenOutput{Text: "Error: Maximum turns reached."}, nil
}

func (g *DataAdmin) EstimateCost(inTokens, outTokens int) float64 {
	return g.brain.EstimateCost(inTokens, outTokens)
}
