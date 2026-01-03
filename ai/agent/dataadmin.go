package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/jsondb"
)

// DataAdminAgent is a specialized agent for database administration tasks.
// It implements the ai.Agent interface.
type DataAdminAgent struct {
	Config            Config
	brain             ai.Generator
	enableObfuscation bool
	registry          *Registry
	databases         map[string]sop.DatabaseOptions
	systemDB          *database.Database
	lastToolCall      *ai.MacroStep

	// API Keys for dynamic switching
	geminiKey string
	openAIKey string
}

// SetGenerator sets the generator for the agent.
func (a *DataAdminAgent) SetGenerator(gen ai.Generator) {
	a.brain = gen
}

// NewDataAdminAgent creates a new instance of DataAdminAgent.
func NewDataAdminAgent(cfg Config, databases map[string]sop.DatabaseOptions, systemDB *database.Database) *DataAdminAgent {
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
		log.Warn("Failed to initialize DataAdmin brain", "error", err)
	}

	agent := &DataAdminAgent{
		Config:            cfg,
		brain:             gen,
		enableObfuscation: cfg.EnableObfuscation,
		databases:         databases,
		systemDB:          systemDB,
		geminiKey:         geminiKey,
		openAIKey:         openAIKey,
	}
	agent.registerTools()
	return agent
}

// SetVerbose enables or disables verbose output.
func (a *DataAdminAgent) SetVerbose(v bool) {
	a.Config.Verbose = v
}

// Open initializes the agent's resources.
func (a *DataAdminAgent) Open(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return nil
	}
	// If CurrentDB is set, start a transaction
	if p.CurrentDB != "" {
		dbName := p.CurrentDB
		// If CurrentDB is a string, look it up in the known databases
		if dbOpts, ok := a.databases[dbName]; ok {
			db := database.NewDatabase(dbOpts)
			if p.Transaction == nil {
				tx, err := db.BeginTransaction(ctx, sop.ForWriting)
				if err != nil {
					return fmt.Errorf("failed to begin transaction on database '%s': %w", dbName, err)
				}
				p.Transaction = tx
			}
		} else {
			return fmt.Errorf("database '%s' not found in agent configuration", dbName)
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
	// cfg := ai.NewAskConfig(opts...)

	// Determine Generator to use (Dynamic Switching)
	gen := a.brain
	if providerOverride, ok := ctx.Value(ai.CtxKeyProvider).(string); ok && providerOverride != "" {
		var err error
		var tempGen ai.Generator

		switch providerOverride {
		case "gemini":
			if a.geminiKey != "" {
				model := os.Getenv("GEMINI_MODEL")
				if model == "" {
					model = "gemini-2.5-flash"
				}
				tempGen, err = generator.New("gemini", map[string]any{
					"api_key": a.geminiKey,
					"model":   model,
				})
			}
		case "chatgpt":
			if a.openAIKey != "" {
				model := os.Getenv("OPENAI_MODEL")
				if model == "" {
					model = "gpt-4o"
				}
				tempGen, err = generator.New("chatgpt", map[string]any{
					"api_key": a.openAIKey,
					"model":   model,
				})
			}
		case "ollama":
			model := os.Getenv("OLLAMA_MODEL")
			if model == "" {
				model = "llama3"
			}
			host := os.Getenv("OLLAMA_HOST")
			if host == "" {
				host = "http://localhost:11434"
			}
			tempGen, err = generator.New("ollama", map[string]any{
				"base_url": host,
				"model":    model,
			})
		}

		if err == nil && tempGen != nil {
			gen = tempGen
		} else {
			log.Warn("Failed to switch provider", "provider", providerOverride, "error", err)
		}
	}

	if gen == nil {
		return "Error: No AI Provider configured. Set GEMINI_API_KEY or OPENAI_API_KEY.", nil
	}

	// 1. Construct System Prompt with Tools
	toolsDef := a.registry.GeneratePrompt()

	// Append Macros as Tools
	if a.systemDB != nil {
		// We need a transaction to read from system DB
		tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
		if err == nil {
			store, err := a.systemDB.OpenModelStore(ctx, "macros", tx)
			if err == nil {
				names, err := store.List(ctx, "general")
				if err == nil {
					for _, name := range names {
						var macro ai.Macro
						if err := store.Load(ctx, "general", name, &macro); err == nil {
							// Format args schema
							argsSchema := "()"
							if len(macro.Parameters) > 0 {
								var params []string
								for _, p := range macro.Parameters {
									params = append(params, fmt.Sprintf("%s: string", p))
								}
								argsSchema = fmt.Sprintf("(%s)", strings.Join(params, ", "))
							}
							toolsDef += fmt.Sprintf("- %s: %s %s\n", macro.Name, macro.Description, argsSchema)
						}
					}
				}
			}
			tx.Commit(ctx)
		}
	}

	// Inject Current Database Schema/Stores
	if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
		var db *database.Database
		if p.CurrentDB == "system" {
			db = a.systemDB
		} else if dbOpts, ok := a.databases[p.CurrentDB]; ok {
			db = database.NewDatabase(dbOpts)
		}

		if db != nil {
			// We use a read-only transaction for this metadata fetch
			if tx, err := db.BeginTransaction(ctx, sop.ForReading); err == nil {
				if stores, err := tx.GetStores(ctx); err == nil {
					toolsDef += fmt.Sprintf("\nActive Database: %s\nAvailable Stores:\n", p.CurrentDB)
					for _, s := range stores {
						var schemaInfo string
						// Try to open store to get schema info
						if storeAccessor, err := jsondb.OpenStore(ctx, db.Config(), s, tx); err == nil {
							info := storeAccessor.GetStoreInfo()
							if info.MapKeyIndexSpecification != "" {
								schemaInfo = fmt.Sprintf(" (Key Schema: %s)", info.MapKeyIndexSpecification)
							}
						}
						toolsDef += fmt.Sprintf("- %s%s\n", s, schemaInfo)
					}
				}
				tx.Rollback(ctx)
			}
		}
	}

	// Append instructions
	toolsDef += `
IMPORTANT:
- The 'select' tool returns the raw data string. You MUST include this raw data in your final response.
- When filtering with 'select', use MongoDB-style operators ($eq, $ne, $gt, $gte, $lt, $lte) for comparisons. Example: {"age": {"$gt": 18}}.
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
		resp, err := gen.Generate(ctx, history, ai.GenOptions{})
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
			cleanText := strings.TrimPrefix(text, "```json")
			cleanText = strings.TrimPrefix(cleanText, "```")
			cleanText = strings.TrimSuffix(cleanText, "```")
			cleanText = strings.TrimSpace(cleanText)

			type ToolCall struct {
				Tool string         `json:"tool"`
				Args map[string]any `json:"args"`
			}
			var toolCalls []ToolCall

			// Try unmarshal as array
			if err := json.Unmarshal([]byte(cleanText), &toolCalls); err != nil {
				// Try unmarshal as single object
				var single ToolCall
				if err2 := json.Unmarshal([]byte(cleanText), &single); err2 == nil && single.Tool != "" {
					toolCalls = []ToolCall{single}
				}
			}

			if len(toolCalls) > 0 {
				if a.Config.Verbose {
					// Display Tool Instructions
					if w, ok := ctx.Value(ai.CtxKeyWriter).(io.Writer); ok {
						var prettyJSON bytes.Buffer
						if err := json.Indent(&prettyJSON, []byte(cleanText), "", "  "); err == nil {
							fmt.Fprintf(w, "\n[Tool Instructions]:\n%s\n", prettyJSON.String())
						} else {
							fmt.Fprintf(w, "\n[Tool Instructions]:\n%s\n", cleanText)
						}
					}
				}

				// Reset LastInteractionSteps
				if p := ai.GetSessionPayload(ctx); p != nil {
					p.LastInteractionSteps = 0
				}

				var results []string
				for _, tc := range toolCalls {
					// Execute Tool
					// We need to execute the tool against the session payload
					result, err := a.Execute(ctx, tc.Tool, tc.Args)
					if err != nil {
						result = "Error: " + err.Error()
					}
					results = append(results, result)

					// Record the tool call if a recorder is present
					if recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
						recorder.RecordStep(ctx, ai.MacroStep{
							Type:    "command",
							Command: tc.Tool,
							Args:    tc.Args,
						})
						if p := ai.GetSessionPayload(ctx); p != nil {
							p.LastInteractionSteps++
						}
					}
				}

				// Return tool output directly (Optimization)
				return strings.Join(results, "\n"), nil
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

// ListTools returns the list of available tools.
func (a *DataAdminAgent) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	// TODO: Implement proper tool listing from registry
	return []ai.ToolDefinition{}, nil
}

// Execute executes the requested tool against the session payload.
func (a *DataAdminAgent) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	// De-obfuscate Args if enabled
	if a.enableObfuscation {
		a.deobfuscateMap(args)
	}

	// Save as Last Tool Call (for macro recording/refactoring)
	// We clone args to avoid mutation issues
	// BUT: If the tool is "macro_add_step_from_last", we should NOT overwrite the last tool call yet!
	// We need to let it run using the *previous* last tool call.
	// So we defer the update of lastToolCall until AFTER execution, OR we skip it for meta-tools.

	isMetaTool := toolName == "macro_add_step_from_last"

	if !isMetaTool {
		savedArgs := make(map[string]any)
		for k, v := range args {
			savedArgs[k] = v
		}
		a.lastToolCall = &ai.MacroStep{
			Type:    "command",
			Command: toolName,
			Args:    savedArgs,
		}
	}

	// Get Session Payload
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	// Priority:
	// 1. Explicit 'database' argument in tool call
	// 2. CurrentDB in SessionPayload
	var dbFound bool
	dbName, _ := args["database"].(string)
	if dbName != "" {
		if _, ok := a.databases[dbName]; ok {
			dbFound = true
		}
	} else {
		if p.CurrentDB != "" {
			dbFound = true
		}
	}

	// If explicit database is provided, we might need to update the context/payload for the tool execution
	// so that tools that rely on p.CurrentDB (like toolSelect) see the correct DB.
	if dbName != "" && dbName != p.CurrentDB {
		// Clone payload
		newPayload := *p
		newPayload.CurrentDB = dbName
		// If switching DB, we cannot use the existing transaction
		newPayload.Transaction = nil
		ctx = context.WithValue(ctx, "session_payload", &newPayload)
	}

	if !dbFound && toolName != "list_databases" && toolName != "list_macros" && toolName != "get_macro_details" {
		// Debugging
		var keys []string
		for k := range a.databases {
			keys = append(keys, k)
		}
		return "", fmt.Errorf("database not found or not selected (DataAdmin). Requested: '%s', Available: %v", dbName, keys)
	}

	// Execute specific tool
	if toolDef, ok := a.registry.Get(toolName); ok {
		return toolDef.Handler(ctx, args)
	}

	// Check if it's a macro
	if a.systemDB != nil {
		// Try to load macro
		// We need a transaction to read from system DB
		// But we might already be in a transaction on the user DB?
		// System DB is separate.
		tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
		if err == nil {
			defer tx.Rollback(ctx)
			store, err := a.systemDB.OpenModelStore(ctx, "macros", tx)
			if err == nil {
				var macro ai.Macro
				if err := store.Load(ctx, "general", toolName, &macro); err == nil {
					// Found macro! Execute it.
					return a.runMacro(ctx, macro, args)
				}
			}
		}
	}

	return "", fmt.Errorf("unknown tool: %s", toolName)
}

func (a *DataAdminAgent) runMacro(ctx context.Context, macro ai.Macro, args map[string]any) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Running macro '%s'...\n", macro.Name))

	// Scope for template resolution
	scope := make(map[string]any)
	for k, v := range args {
		scope[k] = v
	}

	for i, step := range macro.Steps {
		if step.Type == "command" {
			// Resolve args
			resolvedArgs := make(map[string]any)
			for k, v := range step.Args {
				if strVal, ok := v.(string); ok {
					resolvedArgs[k] = resolveTemplate(strVal, scope)
				} else {
					resolvedArgs[k] = v
				}
			}

			// Handle Database Override
			stepCtx := ctx
			if step.Database != "" {
				if p := ai.GetSessionPayload(ctx); p != nil {
					// Clone payload to update CurrentDB for this step only
					newPayload := *p
					newPayload.CurrentDB = step.Database
					// Clear transaction if switching DB, as the existing transaction is bound to the old DB
					if p.CurrentDB != step.Database {
						newPayload.Transaction = nil
					}
					stepCtx = context.WithValue(ctx, "session_payload", &newPayload)
				}
			}

			res, err := a.Execute(stepCtx, step.Command, resolvedArgs)
			if err != nil {
				if !step.ContinueOnError {
					return "", fmt.Errorf("step %d (%s) failed: %w", i+1, step.Command, err)
				}
				sb.WriteString(fmt.Sprintf("Step %d failed: %v\n", i+1, err))
			} else {
				sb.WriteString(fmt.Sprintf("Step %d: %s\n", i+1, res))
			}
		} else {
			sb.WriteString(fmt.Sprintf("Skipping step %d (type '%s' not supported in tool execution)\n", i+1, step.Type))
		}
	}
	return sb.String(), nil
}

func (a *DataAdminAgent) runMacroRaw(ctx context.Context, macro ai.Macro, args map[string]any) (string, error) {
	// Scope for template resolution
	scope := make(map[string]any)
	for k, v := range args {
		scope[k] = v
	}

	var lastResult string

	for i, step := range macro.Steps {
		if step.Type == "command" {
			// Resolve args
			resolvedArgs := make(map[string]any)
			for k, v := range step.Args {
				if strVal, ok := v.(string); ok {
					resolvedArgs[k] = resolveTemplate(strVal, scope)
				} else {
					resolvedArgs[k] = v
				}
			}

			// Handle Database Override
			stepCtx := ctx
			if step.Database != "" {
				if p := ai.GetSessionPayload(ctx); p != nil {
					// Clone payload to update CurrentDB for this step only
					newPayload := *p
					newPayload.CurrentDB = step.Database
					// Clear transaction if switching DB, as the existing transaction is bound to the old DB
					if p.CurrentDB != step.Database {
						newPayload.Transaction = nil
					}
					stepCtx = context.WithValue(ctx, "session_payload", &newPayload)
				}
			}

			res, err := a.Execute(stepCtx, step.Command, resolvedArgs)
			if err != nil {
				if !step.ContinueOnError {
					return "", fmt.Errorf("step %d (%s) failed: %w", i+1, step.Command, err)
				}
			}
			lastResult = res
		}
	}
	return lastResult, nil
}

func resolveTemplate(tmplStr string, scope map[string]any) string {
	if tmplStr == "" {
		return ""
	}
	if tmpl, err := template.New("tmpl").Parse(tmplStr); err == nil {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, scope); err == nil {
			return buf.String()
		}
	}
	return tmplStr
}

func (a *DataAdminAgent) deobfuscateMap(m map[string]any) {
	for k, v := range m {
		m[k] = a.deobfuscateValue(v)
	}
}

func (a *DataAdminAgent) deobfuscateValue(v any) any {
	switch val := v.(type) {
	case string:
		s := strings.Trim(val, "*_`")
		s = strings.ReplaceAll(s, "\u00a0", " ")
		s = strings.TrimSpace(s)
		return obfuscation.GlobalObfuscator.Deobfuscate(s)
	case []any:
		for i, item := range val {
			val[i] = a.deobfuscateValue(item)
		}
		return val
	case map[string]any:
		a.deobfuscateMap(val)
		return val
	default:
		return v
	}
}
