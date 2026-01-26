package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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
	Config       Config
	brain        ai.Generator
	registry     *Registry
	databases    map[string]sop.DatabaseOptions
	systemDB     *database.Database
	lastToolCall *ai.ScriptStep
	service      *Service // Reference back to main service for cache invalidation

	// Session State
	sessionContext *ScriptContext

	// Compiled Scripts Cache
	compiledScripts   map[string]CachedScript
	compiledScriptsMu sync.RWMutex

	// API Keys for dynamic switching
	geminiKey string
	openAIKey string

	// StoreOpener allows mocking the store creation (e.g. for testing)
	StoreOpener func(ctx context.Context, dbOpts sop.DatabaseOptions, storeName string, tx sop.Transaction) (jsondb.StoreAccessor, error)
}

// SetGenerator sets the generator for the agent.
func (a *DataAdminAgent) SetGenerator(gen ai.Generator) {
	a.brain = gen
}

// NewDataAdminAgent creates a new instance of DataAdminAgent.
func NewDataAdminAgent(cfg Config, databases map[string]sop.DatabaseOptions, systemDB *database.Database) *DataAdminAgent {
	// Initialize the "Brain" (Generator)
	// Priority:
	// 1. Configuration passed (from UI/JSON)
	// 2. Environment Variables (Legacy/Docker override)

	var gen ai.Generator
	var err error

	// 1. Try Config First
	if cfg.Generator.Type != "" {
		gen, err = generator.New(cfg.Generator.Type, cfg.Generator.Options)
		if err != nil {
			log.Error(fmt.Sprintf("Failed to initialize generator from config (Type: %s): %v", cfg.Generator.Type, err))
		}
	}

	// 2. Fallback to Environment Variables if Config failed or was empty
	if gen == nil {
		provider := os.Getenv(EnvAIProvider)
		geminiKey := strings.TrimSpace(os.Getenv(EnvGeminiAPIKey))
		openAIKey := strings.TrimSpace(os.Getenv(EnvOpenAIAPIKey))
		llmKey := strings.TrimSpace(os.Getenv(EnvLLMAPIKey))

		// Support unified LLM_API_KEY
		if llmKey != "" {
			// Auto-Correction: If user chose Gemini but provided an OpenAI key (sk-...), switch to ChatGPT
			if provider == ProviderGemini && strings.HasPrefix(llmKey, "sk-") {
				log.Warn("Configuration mismatch: Provider is 'gemini' but LLM_API_KEY is an OpenAI key (sk-...). Switching to 'chatgpt'.")
				provider = ProviderChatGPT
			}

			// If provider is explicitly set, use LLM_API_KEY for that provider
			if provider == ProviderChatGPT && openAIKey == "" {
				openAIKey = llmKey
			} else if provider == ProviderGemini && geminiKey == "" {
				geminiKey = llmKey
			} else if provider == "" {
				// Ambiguous case: Guess provider based on key format or default
				if strings.HasPrefix(llmKey, "sk-") {
					provider = ProviderChatGPT
					openAIKey = llmKey
				} else {
					// Fallback to Gemini (Google API keys usually start with AIza...)
					provider = ProviderGemini
					geminiKey = llmKey
				}
			}
		}
		if provider == "" {
			if openAIKey != "" {
				provider = ProviderChatGPT
			} else if geminiKey != "" {
				provider = ProviderGemini
			}
		}

		if provider == ProviderChatGPT && openAIKey != "" {
			model := os.Getenv(EnvOpenAIModel)
			if model == "" {
				model = DefaultModelOpenAI
			}
			gen, err = generator.New(ProviderChatGPT, map[string]any{
				"api_key": openAIKey,
				"model":   model, // "model" is the correct key for OpenAI generator options
			})
		} else if (provider == ProviderGemini || provider == ProviderLocal || provider == "") && geminiKey != "" {
			model := os.Getenv(EnvGeminiModel)
			if model == "" {
				model = DefaultModelGemini
			}
			gen, err = generator.New(ProviderGemini, map[string]any{
				"api_key": geminiKey,
				"model":   model,
			})
		} else if provider == ProviderOllama {
			model := os.Getenv(EnvOllamaModel)
			if model == "" {
				model = DefaultModelOllama
			}
			host := os.Getenv(EnvOllamaHost)
			if host == "" {
				host = DefaultHost
			}
			gen, err = generator.New(ProviderOllama, map[string]any{
				"base_url": host,
				"model":    model,
			})
		}
	}

	if err != nil {
		log.Error("Failed to initialize AI generator", "error", err)
	}

	agent := &DataAdminAgent{
		Config:    cfg,
		brain:     gen,
		registry:  NewRegistry(),
		databases: databases,
		systemDB:  systemDB,
		// API Keys are less relevant now that we use the Generator interface mostly,
		// but we keep them empty or fill them if we really need them for direct calls later.
		// geminiKey:       geminiKey,
		// openAIKey:       openAIKey,
		sessionContext:  NewScriptContext(),
		compiledScripts: make(map[string]CachedScript),
	}
	// Tools are registered dynamically in Open() or Ask() to ensure context propagation
	// context.Background() is used here to ensure tools are available even if Open() is not called or context context is missing
	agent.registerTools(context.Background())

	return agent
}

// SetService sets the reference to the main service (for cache invalidation).
func (a *DataAdminAgent) SetService(s *Service) {
	a.service = s
}

// shouldObfuscate determines if obfuscation should be applied for a given database.
func (a *DataAdminAgent) shouldObfuscate(dbName string) bool {
	// Look up database options
	if opts, ok := a.databases[dbName]; ok {
		return opts.EnableObfuscation
	}
	// Fallback to mode if no DB options found (e.g. legacy or system)
	// But usually systemDB is also in databases map.
	return false
}

// SetVerbose enables or disables verbose output.
func (a *DataAdminAgent) SetVerbose(v bool) {
	a.Config.Verbose = v
}

// Open initializes the agent's resources.
func (a *DataAdminAgent) Open(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)

	// If CurrentDB is set, start a transaction
	if p != nil && p.CurrentDB != "" {
		dbName := p.CurrentDB

		log.Debug(fmt.Sprintf("DataAdminAgent.Open: Checking DB '%s', SystemDB available: %v", dbName, a.systemDB != nil))

		// Check for system DB
		if dbName == SystemDBName && a.systemDB != nil {
			if p.Transaction == nil {
				tx, err := a.systemDB.BeginTransaction(ctx, sop.ForWriting)
				if err != nil {
					return fmt.Errorf("failed to begin transaction on system database: %w", err)
				}
				p.Transaction = tx
			}
		} else if dbOpts, ok := a.databases[dbName]; ok {
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

	// Register tools now that we have a context (and potentially transactions)
	a.registerTools(ctx)

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
	// Reset session context for each new Ask interaction to prevent variable leakage from previous queries.
	// This ensures that variables like 'output' or 'result' do not carry over stale data.
	a.sessionContext = NewScriptContext()

	// Refresh tools to ensure latest instructions from llm_knowledge are used
	a.registerTools(ctx)

	// Determine Generator to use (Dynamic Switching)
	// We do this BEFORE slash commands now to allow fallback logic
	gen := a.brain
	if providerOverride, ok := ctx.Value(ai.CtxKeyProvider).(string); ok && providerOverride != "" {
		var err error
		var tempGen ai.Generator

		switch providerOverride {
		case ProviderGemini:
			if a.geminiKey != "" {
				model := os.Getenv(EnvGeminiModel)
				if model == "" {
					model = DefaultModelGemini
				}
				tempGen, err = generator.New(ProviderGemini, map[string]any{
					"api_key": a.geminiKey,
					"model":   model,
				})
			}
		case ProviderChatGPT:
			if a.openAIKey != "" {
				model := os.Getenv(EnvOpenAIModel)
				if model == "" {
					model = DefaultModelOpenAI
				}
				tempGen, err = generator.New(ProviderChatGPT, map[string]any{
					"api_key": a.openAIKey,
					"model":   model,
				})
			}
		case ProviderOllama:
			model := os.Getenv(EnvOllamaModel)
			if model == "" {
				model = DefaultModelOllama
			}
			host := os.Getenv(EnvOllamaHost)
			if host == "" {
				host = DefaultHost
			}
			tempGen, err = generator.New(ProviderOllama, map[string]any{
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

	// Handle Direct Tool Invocations (Slash Commands)
	// This allows using tools even if the LLM is disabled (no API Key)
	if strings.HasPrefix(strings.TrimSpace(query), "/") {
		cmdLine := strings.TrimSpace(query)[1:] // Remove leading slash

		// Parse command line respecting quotes: tool_name key="value with spaces"
		toolName, args, err := parseSlashCommand(cmdLine)
		if err == nil && toolName != "" {
			// Execute straight away
			res, execErr := a.Execute(ctx, toolName, args)

			// Fallback Logic:
			// If execution failed AND we have an LLM, maybe the user meant a natural language query starting with /
			// or the parser was too naive. We delegate to the Brain.
			if execErr != nil {
				if gen != nil {
					log.Warn("Slash command failed locally, falling back to LLM", "tool", toolName, "error", execErr)
					// Proceed to LLM flow below...
				} else {
					// No Brain to save us. Die.
					return fmt.Sprintf("Error executing command '%s' (and no AI Copilot available to interpret it): %v", toolName, execErr), nil
				}
			} else {
				return res, nil
			}
		}
	}

	// cfg := ai.NewAskConfig(opts...)

	if gen == nil {
		return "⚠️ **AI Copilot Disabled**: No valid API Key found.\n\nPlease go to **Environment Settings** (HDD icon in bottom left) -> **LLM API Key** to configure your Google Gemini or OpenAI key.", nil
	}

	// 1. Construct System Prompt with Tools
	toolsDef := a.registry.GeneratePrompt()

	// Append Scripts as Tools
	if a.systemDB != nil {
		// We need a transaction to read from system DB
		tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
		if err == nil {
			store, err := a.systemDB.OpenModelStore(ctx, "scripts", tx)
			if err == nil {
				names, err := store.List(ctx, "general")
				if err == nil {
					for _, name := range names {
						var script ai.Script
						if err := store.Load(ctx, "general", name, &script); err == nil {
							// Format args schema
							argsSchema := "()"
							if len(script.Parameters) > 0 {
								var params []string
								for _, p := range script.Parameters {
									params = append(params, fmt.Sprintf("%s: string", p))
								}
								argsSchema = fmt.Sprintf("(%s)", strings.Join(params, ", "))
							}
							toolsDef += fmt.Sprintf("- %s: %s %s\n", script.Name, script.Description, argsSchema)
						}
					}
				}
			}
			tx.Commit(ctx)
		}
	}

	// Inject Learned Knowledge (Contextual)
	if knowledge := RetrieveLLMKnowledge(ctx, a.systemDB); knowledge != "" {
		toolsDef += "\nContext Section (Learned Knowledge):\n" + knowledge
	}

	// Inject Current Database Schema/Stores
	if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
		var db *database.Database
		if p.CurrentDB == SystemDBName {
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
							// SAMPLE SCHEMA (SCALABILITY: Only 1st item)
							if ok, _ := storeAccessor.First(ctx); ok {
								key := storeAccessor.GetCurrentKey()
								if key != nil {
									if val, err := storeAccessor.GetCurrentValue(ctx); err == nil {
										flat := flattenItem(key, val)
										sMap := inferSchema(flat)
										schemaInfo += fmt.Sprintf(" %s", formatSchema(sMap))
									}
								}
							}
							if len(info.Relations) > 0 {
								var rels []string
								for _, r := range info.Relations {
									rels = append(rels, fmt.Sprintf("[%s] -> %s([%s])", strings.Join(r.SourceFields, ", "), r.TargetStore, strings.Join(r.TargetFields, ", ")))
								}
								schemaInfo += fmt.Sprintf(" (Relations: %s)", strings.Join(rels, "; "))
							}
							if info.MapKeyIndexSpecification != "" {
								schemaInfo += fmt.Sprintf(" (Key Schema: %s)", info.MapKeyIndexSpecification)
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
	defaultInst := `
IMPORTANT:
- The 'select' tool returns the raw data string. You MUST include this raw data in your final response.
- When filtering with 'select', use MongoDB-style operators ($eq, $ne, $gt, $gte, $lt, $lte) for comparisons. Example: {"age": {"$gt": 18}}.
- Sorting/Ordering is ONLY supported by the store's Key or a prefix of the Key. You CANNOT sort by arbitrary fields (e.g. "salary", "date") unless they are the Key.
- Check if a secondary index store exists (e.g. 'users_by_age' -> Index of users by age). If so, use it to fulfill sort/filter requests by joining it with the main store (e.g. scan 'users_by_age' and join 'users').
- If no index exists, explain that SOP only supports sorting by Key.
- For complex queries involving joins or multiple steps, use the 'execute_script' tool. The 'script' argument MUST be a valid JSON array of instruction objects (e.g. [{"op": "...", "args": {...}}]).
- When using 'execute_script', the 'script' argument MUST be a valid JSON array of instruction objects. Do NOT leave it empty.
- Join Strategy:
  - Use 'inner' (default) when the query implies "intersection" or strict matching (e.g. "Find orders for user X").
  - Use 'left' (Left Outer Join) when the query implies "optional" relationships (e.g. "List users and their orders, if any").
  - Use 'right' or 'full' only if explicitly requested or logically required to preserve the "right" side or "both" sides.
- Contextual Projection:
  - When joining entities, ALWAYS project identifying fields (e.g. Name, Email) from the parent/source entity alongside the child data in the final result.
  - Do NOT return orphaned child records without their parent's context if the user filtered by the parent.

CONVERSATION VS ACTION:
- Distinguish between a request to PERFORM an action (e.g. "Add a user", "Find records") and a request to GENERATE data or EXPLAIN concepts (e.g. "Give me a new UUID", "How does this work?").
- If the user asks for a "new UUID" or "random ID" in isolation, simply generate it and reply with the text. Do NOT add it to any store unless explicitly instructed to "save" or "add" it.
- Engage in conversation freely to clarify intent before taking destructive or additive actions.

CLIENT_SIDE ACTIONS:
- To switch the active database context in the UI, do NOT use a tool. Instead, strictly output the following text in your final response: [[SWITCH_DATABASE: <db_name>]]. The frontend will detect this and perform the switch.

SELF-CORRECTION & LEARNING:
- **Decision Protocol (Context vs Research)**:
  1. **Analyze Context**: Do you have all necessary schemas, definitions, and rules in the current prompt to answer the user?
  2. **If YES**: Proceed immediately. Do NOT run 'manage_knowledge' needlessly.
  3. **If NO**: Check the "Additional Knowledge Categories" list. If a category is relevant (e.g. user asks about 'Q3 targets' and you see 'sales_targets'), THEN run 'manage_knowledge(action=list, ...)' to fetch it.
  4. **Ambiguity**: If a term is ambiguous (e.g. 'active user') and you lack a definition, consult the 'term' or domain-specific category.

- **Categorization Rule**: When saving new knowledge, do NOT dump everything into 'term'. Create specific namespaces.
  - GOOD: namespace="finance", key="fiscal_year_start"
  - BAD: namespace="term", key="finance_fiscal_year_start"
- **Schema Corrections**: If you discover a schema discrepancy (e.g., 'total_amount' vs 'total'), ALWAYS use namespace="schema".
- **General Logic**: Use "memory" or "term" only for general instructions or glossary definitions.
- Example: manage_knowledge(namespace="finance", key="q1_definition", value="Jan-Mar", action="upsert").
- This allows you to remember this for future queries (e.g., "Find active users").
`
	toolsDef += a.getSystemInstructions(ctx, defaultInst)
	fullPrompt := toolsDef + "\n" + query

	// Obfuscate Prompt if enabled
	// Note: We use the session payload's CurrentDB to decide, checking "shouldObfuscate".
	// If no DB is selected yet, we might fallback to global setting or skip.
	currentDB := ""
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDB = p.CurrentDB
	}

	if a.shouldObfuscate(currentDB) {
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
					if recorder, ok := ctx.Value(ai.CtxKeyScriptRecorder).(ai.ScriptRecorder); ok {
						recorder.RecordStep(ctx, ai.ScriptStep{
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
		// Check if obfuscation was likely used based on our config logic
		// We use the same DB check as before
		currentDB := ""
		if p := ai.GetSessionPayload(ctx); p != nil {
			currentDB = p.CurrentDB
		}
		if a.shouldObfuscate(currentDB) {
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
	// Determine if we should deobfuscate
	dbName, _ := args["database"].(string)
	if dbName == "" {
		if p := ai.GetSessionPayload(ctx); p != nil {
			dbName = p.CurrentDB
		}
	}

	shouldDeobfuscate := a.shouldObfuscate(dbName)

	// De-obfuscate Args if enabled
	if shouldDeobfuscate {
		// Log before deobfuscation
		if b, err := json.Marshal(args); err == nil {
			log.Debug(fmt.Sprintf("Args before deobfuscation: %s", string(b)))
		}

		a.deobfuscateMap(args)

		// Log after deobfuscation
		if b, err := json.MarshalIndent(args, "", "  "); err == nil {
			log.Debug(fmt.Sprintf("Args after deobfuscation: %s", string(b)))
		}
	}

	// Save as Last Tool Call (for script drafting/refactoring)
	// We clone args to avoid mutation issues
	// BUT: If the tool is "add_step_from_last", we should NOT overwrite the last tool call yet!
	// We need to let it run using the *previous* last tool call.
	// So we defer the update of lastToolCall until AFTER execution, OR we skip it for meta-tools.

	isMetaTool := toolName == "add_step_from_last"

	savedArgs := deepCopyMap(args)

	if !isMetaTool {
		a.lastToolCall = &ai.ScriptStep{
			Type:    "command",
			Command: toolName,
			Args:    savedArgs,
		}
	}

	// Notify Recorder (Service) if available
	// This ensures that the Service knows about the tool execution for /last-tool and drafting
	if recorder, ok := ctx.Value(ai.CtxKeyScriptRecorder).(ai.ScriptRecorder); ok {
		// Debug: Check script content
		if script, ok := savedArgs["script"]; ok {
			log.Debug(fmt.Sprintf("Drafting script step. Type: %T, Value: %+v", script, script))
		}

		// We record it even if it's a meta-tool, because from the Service's perspective, it's an action.
		// However, for "add_step_from_last", the user might want to see the *previous* tool.
		// But strictly speaking, "last-tool" should show the LAST executed tool.
		recorder.RecordStep(ctx, ai.ScriptStep{
			Type:    "command",
			Command: toolName,
			Args:    savedArgs,
		})
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
	dbName, _ = args["database"].(string)
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
		ctx = context.WithValue(ctx, SessionPayloadKey, &newPayload)
	}

	if !dbFound && toolName != "list_databases" && toolName != "list_scripts" && toolName != "get_script_details" {
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

	// Dump registry keys if tool not found (Debug)
	var keys []string
	for _, t := range a.registry.List() {
		keys = append(keys, t.Name)
	}
	log.Debug(fmt.Sprintf("Tool '%s' not found. Available tools: %v", toolName, keys))

	// Check if it's a script
	if a.systemDB != nil {
		// Try to load script
		// We need a transaction to read from system DB
		// But we might already be in a transaction on the user DB?
		// System DB is separate.
		tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
		if err == nil {
			defer tx.Rollback(ctx)
			store, err := a.systemDB.OpenModelStore(ctx, "scripts", tx)
			if err == nil {
				var script ai.Script
				if err := store.Load(ctx, "general", toolName, &script); err == nil {
					// Found script! Execute it.
					return a.runScript(ctx, script, args)
				}
			}
		}
	}

	return "", fmt.Errorf("unknown tool: %s", toolName)
}

func (a *DataAdminAgent) runScript(ctx context.Context, script ai.Script, args map[string]any) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Running script '%s'...\n", script.Name))

	// Scope for template resolution
	scope := make(map[string]any)
	for k, v := range args {
		scope[k] = v
	}

	// Transaction Handling
	var tx sop.Transaction
	var commitFunc func() error
	var rollbackFunc func()

	// Default to "manual" (let steps handle it or caller) unless specified
	// The Script struct has TransactionMode field.
	// Values: "none" (default), "single" (global tx), "per_step" (not implemented here, steps do it naturally if no global tx)

	if script.TransactionMode == "single" {
		// Identify Target DB
		// Need a database to start transaction on.
		dbName := script.Database
		if dbName == "" {
			if p := ai.GetSessionPayload(ctx); p != nil {
				dbName = p.CurrentDB
			}
		}
		if dbName == "" {
			dbName = SystemDBName // fallback
		}

		db, err := a.resolveDatabase(dbName)
		if err != nil {
			return "", fmt.Errorf("failed to resolve database '%s' for global transaction: %w", dbName, err)
		}

		tx, err = db.BeginTransaction(ctx, sop.ForWriting) // Assert Write for global scripts usually
		if err != nil {
			return "", fmt.Errorf("failed to begin global transaction: %w", err)
		}

		sb.WriteString(fmt.Sprintf("Global Transaction Started (%s)\n", dbName))

		rollbackFunc = func() {
			tx.Rollback(ctx)
		}
		commitFunc = func() error {
			return tx.Commit(ctx)
		}

		// Inject into Context
		if p := ai.GetSessionPayload(ctx); p != nil {
			// Clone payload
			newPayload := *p
			if newPayload.Transactions == nil {
				newPayload.Transactions = make(map[string]any)
			}
			newPayload.Transactions[dbName] = tx
			newPayload.Transaction = tx           // Legacy field for tools that don't support multi-db yet
			newPayload.ExplicitTransaction = true // Prevent tools from auto-committing
			ctx = context.WithValue(ctx, SessionPayloadKey, &newPayload)
		}
	}

	defer func() {
		if rollbackFunc != nil {
			rollbackFunc() // No-op if committed
		}
	}()

	for i, step := range script.Steps {
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
					stepCtx = context.WithValue(ctx, SessionPayloadKey, &newPayload)
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

	if commitFunc != nil {
		if err := commitFunc(); err != nil {
			return "", fmt.Errorf("failed to commit global transaction: %w", err)
		}
		// Clear rollbackFunc so defer doesn't rollback
		rollbackFunc = nil
	}

	return sb.String(), nil
}

func (a *DataAdminAgent) runScriptRaw(ctx context.Context, script ai.Script, args map[string]any) (string, error) {
	// Scope for template resolution
	scope := make(map[string]any)
	for k, v := range args {
		scope[k] = v
	}

	var lastResult string

	for i, step := range script.Steps {
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
					stepCtx = context.WithValue(ctx, SessionPayloadKey, &newPayload)
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
	// We need to handle key deobfuscation which usually requires removing the old key and adding the new one.
	// Since we can't safely modify keys during range, we collect changes first.
	type keyChange struct {
		oldKey string
		newKey string
		value  any
	}
	var changes []keyChange

	for k, v := range m {
		// 1. Deobfuscate Value (Recursive)
		newVal := a.deobfuscateValue(v)

		// 2. Deobfuscate Key
		newKey := obfuscation.GlobalObfuscator.DeobfuscateText(k)

		if newKey != k {
			changes = append(changes, keyChange{
				oldKey: k,
				newKey: newKey,
				value:  newVal,
			})
		} else {
			// If key didn't change, just update value in place
			m[k] = newVal
		}
	}

	// Apply Key Changes
	for _, c := range changes {
		delete(m, c.oldKey)
		m[c.newKey] = c.value
	}
}

func (a *DataAdminAgent) deobfuscateValue(v any) any {
	switch val := v.(type) {
	case string:
		s := strings.Trim(val, "*_`")
		s = strings.ReplaceAll(s, "\u00a0", " ")
		s = strings.TrimSpace(s)
		return obfuscation.GlobalObfuscator.DeobfuscateText(s)
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

func deepCopyMap(m map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range m {
		result[k] = deepCopyValue(v)
	}
	return result
}

func deepCopyValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return deepCopyMap(val)
	case []any:
		newSlice := make([]any, len(val))
		for i, item := range val {
			newSlice[i] = deepCopyValue(item)
		}
		return newSlice
	default:
		return v
	}
}
