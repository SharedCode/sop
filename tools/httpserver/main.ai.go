package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	log "log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	aidb "github.com/sharedcode/sop/ai/database"
	_ "github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

// ObfuscationMode defines the global obfuscation policy.
type ObfuscationMode string

const (
	// ObfuscationDisabled means no obfuscation globally.
	ObfuscationDisabled ObfuscationMode = "disabled"
	// ObfuscationPerDatabase means we respect obfuscation flag per database.
	ObfuscationPerDatabase ObfuscationMode = "per_database"
	// ObfuscationAllDatabases means we enforce obfuscation globally.
	ObfuscationAllDatabases ObfuscationMode = "all_databases"
)

func handleAIChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	log.Debug("Received AIChat Request", "body", string(bodyBytes))

	var req struct {
		Message   string `json:"message"`
		Database  string `json:"database"`
		StoreName string `json:"store"`
		Agent     string `json:"agent"`
		Provider  string `json:"provider"`
		Format    string `json:"format"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Invalid JSON body", "error", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Trim inputs to avoid mismatch issues
	req.Database = strings.TrimSpace(req.Database)
	req.StoreName = strings.TrimSpace(req.StoreName)

	// Helper to send NDJSON event
	sendEvent := func(eventType string, payload any) {
		// LOGGING FOR ORDERING
		if eventType == "records" || eventType == "record" || eventType == "preview" {
			// Try to detect if payload has ordered structure
			// Using json.Marshal to mimic wire format
			b, _ := json.Marshal(payload)
			log.Debug("UI: Sending Payload (WIRE)", "type", eventType, "json_preview", string(b))
		} else if eventType == "content" {
			s := fmt.Sprintf("%v", payload)
			log.Debug("UI: Sending Content", "length", len(s))
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		// WriteHeader is idempotent in Go's http server (only first call matters),
		// but we should set headers before the first write.
		// Since we only send one event for now, this is fine.
		// If we stream multiple, we must check if headers were written.
		// But for now, let's just write header if we haven't?
		// Actually, standard library handles WriteHeader(200) implicitly on first Write.
		// But we want to be explicit about 200.

		// Note: We don't check if we already wrote, assuming this helper is used once or
		// we accept that headers are sent on first call.

		if err := json.NewEncoder(w).Encode(map[string]any{
			"type":    eventType,
			"payload": payload,
		}); err != nil {
			log.Error("Failed to encode NDJSON event", "error", err)
			return
		}

		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
	}

	// Validate Database if provided
	if req.Database != "" {
		if _, err := getDBOptions(req.Database); err != nil {
			msg := fmt.Sprintf("Invalid database '%s': %v", req.Database, err)
			log.Info("Response: Invalid Database", "error", msg)
			sendEvent("error", msg)
			return
		}
	}

	// Default to the RAG Agent "sql_admin" if not specified
	if req.Agent == "" {
		req.Agent = "sql_admin"
	}

	// Check if a specific RAG Agent is requested
	agentSvc, exists := loadedAgents[req.Agent]
	if !exists {
		msg := fmt.Sprintf("Agent '%s' is not initialized or not found.", req.Agent)
		log.Info("Response: Agent Not Found", "error", msg)
		sendEvent("error", msg)
		return
	}

	ctx := context.Background()
	// Pass provider override via context
	if req.Provider != "" {
		ctx = context.WithValue(ctx, ai.CtxKeyProvider, req.Provider)
	}
	// Pass ToolExecutor via context
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &DefaultToolExecutor{Agents: loadedAgents})

	var askOpts []ai.Option

	// Pass Database via options if provided
	if req.Database != "" {
		// We pass the database name as string. The agent service will resolve it.
		askOpts = append(askOpts, ai.WithDatabase(req.Database))
	}

	// Set Default Format (default to CSV if not specified)
	format := req.Format
	if format == "" {
		format = "csv"
	}
	askOpts = append(askOpts, ai.WithDefaultFormat(format))

	// Construct SessionPayload
	// For now, we populate Databases with just the current one if available,
	// or we could list all available DBs if we want to support cross-db queries.
	// Populate all known DBs from config
	databases := make(map[string]any)
	for _, dbCfg := range config.Databases {
		if opts, err := getDBOptions(dbCfg.Name); err == nil {
			databases[dbCfg.Name] = opts
		}
	}
	// Ensure the requested database is also there (it should be in config, but just in case)
	if req.Database != "" {
		// If we already have it as options, great. If not, we might need to add it?
		// But getDBOptions(req.Database) should have covered it if it's valid.
		// If it's not in config.Databases but getDBOptions works (e.g. dynamic?), add it.
		if _, exists := databases[req.Database]; !exists {
			if opts, err := getDBOptions(req.Database); err == nil {
				databases[req.Database] = opts
			}
		}
	}

	// Also add "system" db if needed?

	payload := &ai.SessionPayload{
		CurrentDB: req.Database,
	}
	askOpts = append(askOpts, ai.WithSessionPayload(payload))

	// Register resources so the agent knows them for obfuscation
	if req.Database != "" {
		obfuscation.GlobalObfuscator.RegisterResource(req.Database, "DB")
	}
	if req.StoreName != "" {
		obfuscation.GlobalObfuscator.RegisterResource(req.StoreName, "STORE")
	}

	// Prepend context information to the message
	fullMessage := req.Message
	// Only prepend context if it's not a system command
	if req.Database != "" && !strings.HasPrefix(req.Message, "/") {
		fullMessage = fmt.Sprintf("Current Database: %s\n%s", req.Database, req.Message)
	}

	// Inject payload into context for Open/Close
	ctx = context.WithValue(ctx, "session_payload", payload)

	// Initialize Agent Session (Transaction)
	if err := agentSvc.Open(ctx); err != nil {
		msg := fmt.Sprintf("Agent '%s' failed to open session: %v", req.Agent, err)
		log.Error("Response: Session Open Failed", "error", msg)
		sendEvent("error", msg)
		return
	}
	defer func() {
		if err := agentSvc.Close(ctx); err != nil {
			log.Error(fmt.Sprintf("Agent '%s' failed to close session: %v", req.Agent, err))
		}
	}()

	// agentSvc delegates to dataadmin agent as necessary for LLM ask.
	response, err := agentSvc.Ask(ctx, fullMessage, askOpts...)
	if err != nil {
		msg := fmt.Sprintf("Agent '%s' failed: %v", req.Agent, err)
		log.Error("Response: Agent Ask Failed", "error", msg)
		sendEvent("error", msg)
		return
	}

	// Process the response from LLM as returned by the agent.
	text := strings.TrimSpace(response)
	var toolCall struct {
		Tool string         `json:"tool"`
		Args map[string]any `json:"args"`
	}
	cleanText := strings.TrimPrefix(text, "```json")
	cleanText = strings.TrimPrefix(cleanText, "```")
	cleanText = strings.TrimSuffix(cleanText, "```")
	cleanText = strings.TrimSpace(cleanText)

	if err := json.Unmarshal([]byte(cleanText), &toolCall); err == nil && toolCall.Tool != "" {
		// If we get a raw tool call here, it means the Agent didn't execute it.
		// This shouldn't happen with the new architecture where agents execute their own tools.
		// But if it does (e.g. legacy agent or fallback), we log it.
		log.Debug(fmt.Sprintf("Agent returned raw tool call (unexpected): %s", toolCall.Tool))

		// We no longer execute tools here. We return the raw response.
		// Or should we error?
		// Let's return it as a response so the user sees what the agent tried to do.
		msg := fmt.Sprintf("Agent attempted to call tool '%s' but failed to execute it internally.", toolCall.Tool)
		log.Info("Response: Raw Tool Call", "response", msg)
		sendEvent("content", msg)
		return
	}

	// Try to interpret the response as Data (JSON Array)
	// Many agents return a JSON array of objects for data queries.
	// Try to interpret the response as Data (JSON Array)
	// Many agents return a JSON array of objects for data queries.
	// NOTE: We use RawMessage to preserve field ordering within the records.
	var rawRecords []json.RawMessage
	var mapRecords []map[string]any
	if err := json.Unmarshal([]byte(cleanText), &mapRecords); err == nil && len(mapRecords) > 0 {
		// It's a valid list of maps (objects).
		// Re-unmarshal as RawMessage to get the ordered bytes source
		if err := json.Unmarshal([]byte(cleanText), &rawRecords); err == nil {
			log.Debug("Response: Detected JSON Records", "count", len(rawRecords))
			for _, rec := range rawRecords {
				sendEvent("record", rec)
			}
			return
		}
	}

	// HEURISTIC: Check for Embedded JSON Array in conversational text
	// e.g. "Here is the result: [{...}]"
	if idxStart := strings.Index(cleanText, "["); idxStart >= 0 {
		if idxEnd := strings.LastIndex(cleanText, "]"); idxEnd > idxStart {
			candidate := cleanText[idxStart : idxEnd+1]
			// Try to unmarshal the candidate
			var embeddedMapRecords []map[string]any
			if err := json.Unmarshal([]byte(candidate), &embeddedMapRecords); err == nil && len(embeddedMapRecords) > 0 {
				// Success! It's a valid JSON array embedded in text.
				// Re-unmarshal as RawMessage on the CANDIDATE string
				if err := json.Unmarshal([]byte(candidate), &rawRecords); err == nil {
					log.Debug("Response: Detected Embedded JSON Records", "count", len(rawRecords))
					for _, rec := range rawRecords {
						sendEvent("record", rec)
					}
					return
				}
			}
		}
	}

	// Try to interpret as single object?
	// Sometimes an agent returns a single object.
	var singleRecord map[string]any
	// Ensure it's not just a string disguised as object? No, json.Unmarshal handles that.
	// But check if it starts with '{' to avoid false positives with numbers or strings if cleanText was weird found.
	if strings.HasPrefix(cleanText, "{") {
		if err := json.Unmarshal([]byte(cleanText), &singleRecord); err == nil {
			log.Debug("Response: Detected Single JSON Record")
			// Check if it's a KV pair structure?
			// The UI treats 'kv' as generic key-value, but 'record' works too.
			// Let's just send as record.
			sendEvent("record", singleRecord)
			return
		}
	}

	// Try to interpret as NDJSON (Newline Delimited JSON)
	// This handles script outputs that now default to NDJSON stream format.
	lines := strings.Split(cleanText, "\n")
	var ndjsonEvents []map[string]any
	isNDJSON := false

	// Heuristic: If we have multiple lines and the first non-empty one parses as JSON, try to parse all.
	// Or even if just one line parses? No, "Hello" isn't JSON. "{...}" is.
	// If it contains at least one valid JSON object line, treating it as NDJSON might be valid,
	// but we should fail gracefully if lines are mixed.
	// Actually, if PlayScript returns NDJSON, every line is JSON.
	if len(lines) > 0 {
		validCount := 0
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var obj map[string]any
			if strings.HasPrefix(line, "{") && json.Unmarshal([]byte(line), &obj) == nil {
				ndjsonEvents = append(ndjsonEvents, obj)
				validCount++
			} else {
				// Found a non-JSON line. If we already found JSON, this is mixed content (bad).
				// But maybe it's just some text at the start/end?
				// For now, if we encounter ANY non-JSON line, cancel NDJSON detection to be partial?
				// No, let's treat it as text if it's not 100% NDJSON (ignoring empty lines).
				// Exception: "Error: ..." sometimes creeps in.
				isNDJSON = false
				break
			}
		}
		// If we found valid JSON lines and didn't break (meaning all non-empty lines were JSON)
		if validCount > 0 && len(ndjsonEvents) == validCount {
			isNDJSON = true
		}
	}

	if isNDJSON {
		log.Debug("Response: Detected NDJSON Stream", "count", len(ndjsonEvents))
		for _, evt := range ndjsonEvents {
			// Map specific event types from PlayScript to UI events
			// PlayScript types: "log", "record", "error", "step_start", "outputs"
			// UI events: "record", "content", "error", "tool_call"

			rawType, _ := evt["type"].(string)

			switch rawType {
			case "record":
				if rec, ok := evt["record"]; ok {
					sendEvent("record", rec)
				} else if rec, ok := evt["data"]; ok {
					// Fallback for older scripts
					sendEvent("record", rec)
				}
			case "log":
				msg, _ := evt["message"].(string)
				// Send as markdown content? or console log?
				// UI parses 'content' as markdown.
				// Let's prefix logs given they are debug info usually.
				sendEvent("content", fmt.Sprintf("> *Log:* %s\n", msg))
			case "error":
				msg, _ := evt["error"].(string)
				sendEvent("error", msg)
			case "step_start":
				// Maybe ignore or show simple header?
				name, _ := evt["name"].(string)
				if name == "" {
					name, _ = evt["command"].(string)
				}
				stepIndexVal := evt["step_index"]

				// Handle different types for step_index (float64 if from JSON unmarshal)
				var stepIndex int
				if f, ok := stepIndexVal.(float64); ok {
					stepIndex = int(f)
				} else if i, ok := stepIndexVal.(int); ok {
					stepIndex = i
				}

				if stepIndex > 0 {
					sendEvent("content", fmt.Sprintf("**Step %d:** `%s`\n", stepIndex, name))
				} else {
					sendEvent("content", fmt.Sprintf("**Step:** `%s`\n", name))
				}
			case "outputs":
				// This is usually the final result or step result.
				// If it's a list, we might want to iterate?
				if list, ok := evt["outputs"].([]any); ok {
					// Check if these are records
					for _, item := range list {
						sendEvent("record", item)
					}
				} else {
					// Send as content
					b, _ := json.MarshalIndent(evt["outputs"], "", "  ")
					sendEvent("content", fmt.Sprintf("```json\n%s\n```", string(b)))
				}
			default:
				// Fallback: send the whole event payload as a record or content?
				// If it has 'payload' or 'data', send that.
				// If not, just send the event object itself as a record (for debugging)
				sendEvent("record", evt)
			}
		}
		return
	}

	//log.Debug("Response: Success (Text)", "response", response)

	sendEvent("content", response)
}

func initAgents() {
	loadAgent("sql_admin", "ai/data/sql_admin_pipeline.json")
}

func seedDefaultScripts(db *aidb.Database) {
	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to begin transaction for seeding scripts: %v", err))
		return
	}
	store, err := db.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		tx.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open scripts store: %v", err))
		return
	}

	// Check if demo_loop exists
	// We force update it to ensure latest schema is used during development
	// var existing ai.Script
	// if err := store.Load(ctx, "scripts", "demo_loop", &existing); err == nil {
	// 	tx.Rollback(ctx)
	// 	return // Already exists
	// }

	// Create demo_loop script
	demoLoop := ai.Script{
		Name:        "demo_loop",
		Description: "Demonstrates loops and variables",
		Steps: []ai.ScriptStep{
			{
				Type:     "set",
				Variable: "items",
				Value:    "apple\nbanana\ncherry",
			},
			{
				Type:     "loop",
				List:     "items",
				Iterator: "fruit",
				Steps: []ai.ScriptStep{
					{
						Type:    "say",
						Message: "Processing {{.fruit}}...",
					},
					{
						Type:   "ask",
						Prompt: "What color is {{.fruit}}? (Answer in 1 word)",
					},
				},
			},
		},
	}

	if err := store.Save(ctx, "general", "demo_loop", demoLoop); err != nil {
		log.Error(fmt.Sprintf("Failed to save demo_loop script: %v", err))
		tx.Rollback(ctx)
		return
	}

	tx.Commit(ctx)
	log.Info("Seeded 'demo_loop' script.")
}

// seedLLMKnowledge initializes the system instruction/knowledge store.
func seedLLMKnowledge(db *aidb.Database) {
	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to begin transaction for seeding knowledge: %v", err))
		return
	}

	opts := sop.StoreOptions{
		Name:                     "llm_knowledge",
		IsPrimitiveKey:           false,
		IsValueDataInNodeSegment: true,
	}

	// Use jsondb.IndexSpecification to generating the MapKeyIndexSpecification JSON string.
	// This ensures consistency with other components that check or use this specification.
	idxSpec := jsondb.IndexSpecification{
		IndexFields: []jsondb.IndexFieldSpecification{
			{FieldName: "Category", AscendingSortOrder: true},
			{FieldName: "Name", AscendingSortOrder: true},
		},
	}
	if b, err := json.Marshal(idxSpec); err == nil {
		opts.MapKeyIndexSpecification = string(b)
	}

	// Open generic B-Tree for KnowledgeKey -> string
	// Define comparers for the KnowledgeKey
	comparer := func(a, b agent.KnowledgeKey) int {
		if a.Category < b.Category {
			return -1
		}
		if a.Category > b.Category {
			return 1
		}
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	}

	store, err := database.NewBtree[agent.KnowledgeKey, string](ctx, db.Options(), "llm_knowledge", tx, comparer, opts)
	if err != nil {
		tx.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open llm_knowledge store: %v", err))
		return
	}

	defaultInst := agent.DefaultKnowledge["execute_script"]
	if defaultInst == "" {
		log.Warn("No default instruction found for execute_script")
		tx.Rollback(ctx)
		return
	}

	// Tool knowledge uses "tool" category
	key := agent.KnowledgeKey{Category: "tool", Name: "execute_script"}

	// Upsert to ensure we have the latest built-in knowledge
	if _, err := store.Upsert(ctx, key, defaultInst); err != nil {
		tx.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to upsert execute_script instruction: %v", err))
		return
	}
	log.Info("Seeded/Updated 'execute_script' instruction.")

	tx.Commit(ctx)
}

func loadAgent(key, configPath string) {
	// Try to find the file in common locations
	pathsToTry := []string{
		configPath,
		filepath.Join("..", "..", configPath),            // From tools/httpserver
		filepath.Join("..", configPath),                  // From tools
		filepath.Join("/Users/grecinto/sop", configPath), // Absolute fallback
	}

	var foundPath string
	for _, p := range pathsToTry {
		if _, err := os.Stat(p); err == nil {
			foundPath = p
			break
		}
	}

	if foundPath == "" {
		log.Debug(fmt.Sprintf("Agent config not found at %s (searched parents), skipping.", configPath))
		return
	}

	cfg, err := agent.LoadConfigFromFile(foundPath)
	if err != nil {
		log.Debug(fmt.Sprintf("Failed to load agent config %s: %v", foundPath, err))
		return
	}

	// Apply Stub Mode if enabled globally
	if config.StubMode {
		log.Info(fmt.Sprintf("Enabling Stub Mode for agent %s", key))
		cfg.StubMode = true
		for i := range cfg.Agents {
			cfg.Agents[i].StubMode = true
		}
	}

	// Apply Global LLM API Key if provided in HTTP Config
	if config.LLMApiKey != "" {
		injectAPIKey(cfg, config.LLMApiKey)
	}

	// Apply Global Obfuscation Mode if specified in HTTP Config
	// We do NOT update the agent config anymore, instead we calculate the per-database flag below
	globalObfMode := ObfuscationDisabled
	if config.ObfuscationMode != "" {
		log.Info("Applying Global Obfuscation Mode from HTTP config", "mode", config.ObfuscationMode)
		globalObfMode = ObfuscationMode(config.ObfuscationMode)
	}

	// Ensure absolute path for storage
	if cfg.StoragePath != "" {
		if !filepath.IsAbs(cfg.StoragePath) {
			configDir := filepath.Dir(foundPath)
			cfg.StoragePath = filepath.Join(configDir, cfg.StoragePath)
		}
		if absPath, err := filepath.Abs(cfg.StoragePath); err == nil {
			cfg.StoragePath = absPath
		}
	}

	log.Debug(fmt.Sprintf("Initializing AI Agent: %s (%s)...", cfg.Name, cfg.ID))

	// Initialize System DB
	sysOpts, err := getSystemDBOptions()
	var sysDB *aidb.Database
	if err == nil {
		sysDB = aidb.NewDatabase(sysOpts)
		// Seed default scripts for testing
		seedDefaultScripts(sysDB)
		seedLLMKnowledge(sysDB)
	} else {
		log.Debug(fmt.Sprintf("System DB not available for agent %s: %v", cfg.ID, err))
	}

	// Prepare Databases map
	databases := make(map[string]sop.DatabaseOptions)

	// Add System DB if available
	if sysDB != nil {
		databases["System DB"] = sysOpts
	}

	for _, dbCfg := range config.Databases {
		// We need to use a copy of dbCfg because getDBOptionsFromConfig takes a pointer
		// and loop variable reuse might be an issue in older Go versions, though fixed in 1.22.
		// Safe to just pass address.
		d := dbCfg
		opts, err := getDBOptionsFromConfig(&d)
		if err == nil {
			// Calculate Obfuscation Flag based on Global Mode and Per-DB Config
			switch globalObfMode {
			case ObfuscationAllDatabases:
				opts.EnableObfuscation = true
			case ObfuscationDisabled:
				opts.EnableObfuscation = false
			case ObfuscationPerDatabase:
				opts.EnableObfuscation = dbCfg.EnableObfuscation
			default:
				// Fallback to Disabled if unknown
				opts.EnableObfuscation = false
			}

			databases[d.Name] = opts
		}
	}

	registry := make(map[string]ai.Agent[map[string]any])

	// Helper to initialize an agent from a config
	initAgent := func(agentCfg agent.Config) (ai.Agent[map[string]any], error) {
		if agentCfg.StoragePath != "" {
			if !filepath.IsAbs(agentCfg.StoragePath) {
				configDir := filepath.Dir(foundPath)
				agentCfg.StoragePath = filepath.Join(configDir, agentCfg.StoragePath)
			}
			if absPath, err := filepath.Abs(agentCfg.StoragePath); err == nil {
				agentCfg.StoragePath = absPath
			}
		}
		svc, err := agent.NewFromConfig(context.Background(), agentCfg, agent.Dependencies{
			AgentRegistry: registry,
			SystemDB:      sysDB,
			Databases:     databases,
		})
		return svc, err
	}

	// Pre-register internal policy agents
	for _, pCfg := range cfg.Policies {
		if pCfg.ID != "" {
			registry[pCfg.ID] = agent.NewPolicyAgent(pCfg.ID, nil, nil)
		}
	}

	// Register locally defined agents
	for _, localAgentCfg := range cfg.Agents {
		if localAgentCfg.ID == "" {
			continue
		}
		if _, exists := registry[localAgentCfg.ID]; exists {
			continue
		}

		// Dynamic Generator Override based on Environment Variables
		// This allows the "sql_core" agent to switch between Ollama/Gemini/GPT without changing the JSON file.
		if localAgentCfg.Generator.Type != "" {
			// Check for explicit override from UI (passed via context or global config?)
			// For now, we stick to Env Vars as the "Configuration" source.
			// But to allow runtime switching, we might need to re-initialize the agent or pass the provider in the Ask() call.
			// Since Ask() is generic, we can't easily pass it there without changing the interface.
			// However, we can make the Generator itself dynamic!

			provider := os.Getenv("AI_PROVIDER")
			if provider != "" {
				log.Debug(fmt.Sprintf("Overriding generator for agent %s to %s", localAgentCfg.ID, provider))
				localAgentCfg.Generator.Type = provider
				localAgentCfg.Generator.Options = make(map[string]any) // Clear options to rely on env vars
			}
		}

		log.Debug(fmt.Sprintf("Initializing local agent: %s...", localAgentCfg.ID))
		svc, err := initAgent(localAgentCfg)
		if err != nil {
			log.Debug(fmt.Sprintf("Failed to initialize local agent %s: %v", localAgentCfg.ID, err))
			continue
		}
		registry[localAgentCfg.ID] = svc
		loadedAgents[localAgentCfg.ID] = svc
	}

	// Initialize the main agent
	mainAgent, err := agent.NewFromConfig(context.Background(), *cfg, agent.Dependencies{
		AgentRegistry: registry,
		SystemDB:      sysDB,
		Databases:     databases,
	})
	if err != nil {
		log.Debug(fmt.Sprintf("Failed to initialize main agent %s: %v", key, err))
		return
	}

	// Enable History Injection (Manual Override as requested)
	if svc, ok := mainAgent.(*agent.Service); ok {
		log.Info("Enabling History Injection for agent", "agent", key)
		svc.SetFeature("history_injection", true)
	}

	loadedAgents[key] = mainAgent
	log.Debug(fmt.Sprintf("Agent '%s' initialized successfully.", key))
}

// DefaultToolExecutor implements ai.ToolExecutor by delegating to registered agents.
type DefaultToolExecutor struct {
	Agents map[string]ai.Agent[map[string]any]
}

func (e *DefaultToolExecutor) Execute(ctx context.Context, tool string, args map[string]any) (string, error) {
	// For now, we assume tools are handled by the "sql_core" agent (DataAdminAgent)
	// In a real system, we might look up the tool in a global registry or iterate agents.

	// Try sql_core first
	if agentSvc, ok := e.Agents["sql_core"]; ok {
		if da, ok := agentSvc.(*agent.DataAdminAgent); ok {
			return da.Execute(ctx, tool, args)
		}
	}

	return "", fmt.Errorf("tool '%s' not found or no executor available", tool)
}

func injectAPIKey(cfg *agent.Config, apiKey string) {
	if cfg.Generator.Type == "gemini" {
		if cfg.Generator.Options == nil {
			cfg.Generator.Options = make(map[string]any)
		}
		// Only override if not set (or should we strictly override?)
		// Let's force override for now as the global config is likely the user's intent
		cfg.Generator.Options["api_key"] = apiKey
	}

	// Recursive injection
	for i := range cfg.Agents {
		// We need to take address of slice element to modify it
		// But cfg.Agents is []Config.
		// wait, Config struct has []Config.
		// We can't easily recurse efficiently on value type slices in Go without pointers or rewriting the slice.
		// cfg.Agents is []Config (values).
		subCfg := &cfg.Agents[i]
		injectAPIKey(subCfg, apiKey)
	}
}

func (e *DefaultToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}
