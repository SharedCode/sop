package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
)

// DataAdminAgent is a specialized agent for database administration tasks.
// It implements the ai.Agent interface.
type DataAdminAgent struct {
	Config            Config
	brain             ai.Generator
	enableObfuscation bool
	registry          *Registry
}

// NewDataAdminAgent creates a new instance of DataAdminAgent.
func NewDataAdminAgent(cfg Config) *DataAdminAgent {
	// Initialize the "Brain" (Generator)
	// Logic ported from ai/generator/dataadmin.go
	provider := os.Getenv("AI_PROVIDER")
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

	if provider == "chatgpt" && openAIKey != "" {
		model := os.Getenv("OPENAI_MODEL")
		if model == "" {
			model = "gpt-4o"
		}
		gen, err = generator.New("chatgpt", map[string]any{
			"api_key": openAIKey,
			"model":   model,
		})
	} else if (provider == "gemini" || provider == "local" || provider == "") && geminiKey != "" {
		model := os.Getenv("GEMINI_MODEL")
		if model == "" {
			model = "gemini-2.5-flash"
		}
		gen, err = generator.New("gemini", map[string]any{
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
		gen, err = generator.New("ollama", map[string]any{
			"base_url": host,
			"model":    model,
		})
	}

	if err != nil {
		fmt.Printf("Warning: Failed to initialize DataAdmin brain: %v\n", err)
	}

	agent := &DataAdminAgent{
		Config:            cfg,
		brain:             gen,
		enableObfuscation: cfg.EnableObfuscation,
	}
	agent.registerTools()
	return agent
}

// Open initializes the agent's resources.
func (a *DataAdminAgent) Open(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return nil
	}
	// If CurrentDB is a *database.Database, start a transaction
	if db, ok := p.CurrentDB.(*database.Database); ok {
		// Only start a new transaction if one doesn't exist
		if p.Transaction == nil {
			// We use ForWriting to allow updates by default in a session
			tx, err := db.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			p.Transaction = tx
		}
	}
	return nil
}

// Close cleans up the agent's resources.
func (a *DataAdminAgent) Close(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)
	if p == nil || p.Transaction == nil {
		return nil
	}
	if tx, ok := p.Transaction.(sop.Transaction); ok {
		return tx.Commit(ctx)
	}
	return nil
}

// Search performs a search using the agent's capabilities.
func (a *DataAdminAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return []ai.Hit[map[string]any]{}, nil
}

// Ask processes a query and returns a response.
func (a *DataAdminAgent) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	if a.brain == nil {
		return "Error: No AI Provider configured. Set GEMINI_API_KEY or OPENAI_API_KEY.", nil
	}

	// 1. Construct System Prompt with Tools
	toolsDef := a.registry.GeneratePrompt()

	// Append instructions
	toolsDef += `
IMPORTANT:
- If user asks for "JSON", you MUST set "format": "json".
- The 'select' tool returns the raw data string. You MUST include this raw data in your final response.
`
	fullPrompt := toolsDef + "\n" + query

	// Obfuscate Prompt if enabled
	if a.enableObfuscation {
		fullPrompt = obfuscation.GlobalObfuscator.ObfuscateText(fullPrompt)
	}

	// 2. ReAct Loop
	maxTurns := 5
	history := fullPrompt

	for i := 0; i < maxTurns; i++ {
		resp, err := a.brain.Generate(ctx, history, ai.GenOptions{})
		if err != nil {
			return "", err
		}

		text := strings.TrimSpace(resp.Text)

		// Check for Tool Call
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
				// Record the tool call if a recorder is present
				if recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
					recorder.RecordStep(ai.MacroStep{
						Type:    "command",
						Command: toolCall.Tool,
						Args:    toolCall.Args,
					})
				}

				// Execute Tool
				// We need to execute the tool against the session payload
				result, err := a.executeTool(ctx, toolCall.Tool, toolCall.Args)
				if err != nil {
					result = "Error: " + err.Error()
				}

				// Return tool output directly (Optimization)
				return result, nil
			}
		}

		// De-obfuscate Output Text if enabled
		if a.enableObfuscation {
			text = obfuscation.GlobalObfuscator.DeobfuscateText(text)
		}
		return text, nil
	}

	return "Error: Maximum turns reached.", nil
}

// executeTool executes the requested tool against the session payload.
func (a *DataAdminAgent) executeTool(ctx context.Context, toolName string, args map[string]any) (string, error) {
	// De-obfuscate Args if enabled
	if a.enableObfuscation {
		for k, v := range args {
			if s, ok := v.(string); ok {
				s = strings.Trim(s, "*_`")
				s = strings.ReplaceAll(s, "\u00a0", " ")
				s = strings.TrimSpace(s)
				s = obfuscation.GlobalObfuscator.Deobfuscate(s)
				args[k] = s
			}
		}
	}

	// Get Session Payload
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName != "" {
		if val, ok := p.Databases[dbName]; ok {
			if d, ok := val.(*database.Database); ok {
				db = d
			}
		}
	} else {
		if d, ok := p.CurrentDB.(*database.Database); ok {
			db = d
		}
	}

	if db == nil && toolName != "list_databases" {
		return "", fmt.Errorf("database not found or not selected")
	}

	// Execute specific tool
	if toolDef, ok := a.registry.Get(toolName); ok {
		return toolDef.Handler(ctx, args)
	}

	return "", fmt.Errorf("unknown tool: %s", toolName)
}
