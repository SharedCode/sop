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
	"time"

	"github.com/google/uuid"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	aidb "github.com/sharedcode/sop/ai/database"
	
	_ "github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/memory"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/database"
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

// DirectFlushingWriter writes directly to the http.ResponseWriter and flushes
type DirectFlushingWriter struct {
	w http.ResponseWriter
}

func (d *DirectFlushingWriter) Write(p []byte) (n int, err error) {
	d.w.Header().Set("Content-Type", "application/x-ndjson")
	n, err = d.w.Write(p)
	if f, ok := d.w.(http.Flusher); ok {
		f.Flush()
	}
	return
}

func seedMetaCognitionAsync(userID, kbName string, sysOpts sop.DatabaseOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sysDB := aidb.NewDatabase(sysOpts)

	trans, err := sysDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return
	}

	embedder := GetConfiguredEmbedder(nil)
	kb, err := sysDB.OpenKnowledgeBase(ctx, kbName, trans, nil, embedder)
	if err != nil {
		trans.Rollback(ctx)
		return
	}
	opts := &memory.SearchOptions[map[string]any]{Limit: 1}
	res, _ := kb.SearchKeywords(ctx, "Meta_Cognition", opts)
	trans.Rollback(ctx)

	if len(res) > 0 {
		return
	}

	wTrans, err := sysDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return
	}
	defer wTrans.Rollback(ctx)
	wKb, err := sysDB.OpenKnowledgeBase(ctx, kbName, wTrans, nil, embedder)
	if err != nil {
		return
	}

	thoughtText := "Meta-Memory Rules: 1) Generalize bugs instead of memorizing stack traces. 2) Never duplicate information available in SOP. 3) Prioritize specific user preferences over generic defaults."
	vecs, err := embedder.EmbedTexts(ctx, []string{thoughtText})
	if err == nil && len(vecs) > 0 {
		data := map[string]any{
			"type":        "meta_rule",
			"description": "Base rules for Long Term Memory consolidation",
		}
		_ = wKb.IngestThought(ctx, thoughtText, "Meta_Cognition", "system", vecs[0], data)
		if wTrans.Commit(ctx) == nil {
			log.Info("Successfully seeded lightweight Meta_Cognition to LTM", "user_id", userID)
		}
	}
}

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
		Message     string   `json:"message"`
		Database    string   `json:"database"`
		StoreName   string   `json:"store"`
		Agent       string   `json:"agent"`
		Provider    string   `json:"provider"`
		Format      string   `json:"format"`
		Verbose     bool     `json:"verbose"`
		SessionID   string   `json:"session_id"`
		Domain      string   `json:"domain"`
		SelectedKBs []string `json:"selected_kbs"`
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
		if _, err := getDBOptions(r.Context(), req.Database); err != nil {
			msg := fmt.Sprintf("Invalid database '%s': %v", req.Database, err)
			log.Info("Response: Invalid Database", "error", msg)
			sendEvent("error", msg)
			return
		}
	}

	// Default to the RAG Agent "copilot" if not specified
	if req.Agent == "" {
		req.Agent = "copilot"
	}

	// Check if a specific RAG Agent is requested
	blueprint, exists := loadedAgents[req.Agent]
	if !exists {
		msg := fmt.Sprintf("Agent '%s' is not initialized or not found.", req.Agent)
		log.Info("Response: Agent Not Found", "error", msg)
		sendEvent("error", msg)
		return
	}

	var agentSvc ai.Agent[map[string]any]
	if req.SessionID == "" {
		req.SessionID = uuid.New().String() // Client lifecycle zero-day bootstrapping
	}

	// Provision LTM (memory_<user_id>)
	if sysOpts, err := getSystemDBOptions(r.Context()); err == nil {
		sysDB := aidb.NewDatabase(sysOpts)
		ctx := r.Context()
		kbName := fmt.Sprintf("%s%s", ai.MemoryKBPrefix, req.SessionID)
		trans, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
		if trans != nil {
			// Just open it to ensure it is created
			dbEmbedder := GetConfiguredEmbedder(nil)
					dbLLM := GetConfiguredLLM(nil)

					sysDB.OpenKnowledgeBase(ctx, kbName, trans, dbLLM, dbEmbedder)
			trans.Commit(ctx)
			go seedMetaCognitionAsync(req.SessionID, kbName, sysOpts)
		}
	}

	agentSvc, sessionMu := activeSessions.GetOrCreate(req.SessionID, func() ai.Agent[map[string]any] {
		if cloneable, ok := blueprint.(interface {
			Clone() ai.Agent[map[string]any]
		}); ok {
			log.Debug("Cloned new pristine agent instance for session", "session_id", req.SessionID)
			return cloneable.Clone()
		}
		return blueprint // Fallback if not cloneable
	})

	// Uniqueness / creation has been safely handled.
	// We MUST lock the agent before asking it anything, else concurrent requests on the same ID scramble it!
	sessionMu.Lock()
	defer sessionMu.Unlock()

	// Always enforce uniqueness and tell the client their active session ID
	// BEFORE long LLM wait so they can anchor to it immediately.
	sendEvent("session_id", req.SessionID)

	ctx := r.Context()
	// Pass provider override via context
	if req.Provider != "" {
		ctx = context.WithValue(ctx, ai.CtxKeyProvider, req.Provider)
	}
	// Pass Verbose flag
	if req.Verbose {
		ctx = context.WithValue(ctx, "verbose", true)
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
		if opts, err := getDBOptions(r.Context(), dbCfg.Name); err == nil {
			databases[dbCfg.Name] = opts
		}
	}
	// Ensure the requested database is also there (it should be in config, but just in case)
	if req.Database != "" {
		// If we already have it as options, great. If not, we might need to add it?
		// But getDBOptions(req.Database) should have covered it if it's valid.
		// If it's not in config.Databases but getDBOptions works (e.g. memory?), add it.
		if _, exists := databases[req.Database]; !exists {
			if opts, err := getDBOptions(r.Context(), req.Database); err == nil {
				databases[req.Database] = opts
			}
		}
	}

	// Also add "system" db if needed?

	payload := &ai.SessionPayload{
		CurrentDB:    req.Database,
		ActiveDomain: req.Domain,
		SelectedKBs:  req.SelectedKBs,
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
	msgTrimmed := strings.TrimSpace(req.Message)
	if req.Database != "" && !strings.HasPrefix(msgTrimmed, "/") && msgTrimmed != "last-tool" && msgTrimmed != "last_tool" {
		fullMessage = fmt.Sprintf("Current Database: %s\n%s", req.Database, req.Message)
	}

	// Inject payload into context for Open/Close
	ctx = context.WithValue(ctx, "session_payload", payload)

	// Inject streaming writer so Agent commands (like /run) can stream directly
	// bypassing the blocking Ask() return.
	// We implement the Writer interface but ensure we flush.
	// NOTE: This writes RAW bytes. The client must handle mixed content if Ask()
	// also returns text. But usually if a command streams, it returns empty text.
	streamWriter := &DirectFlushingWriter{w: w}
	ctx = context.WithValue(ctx, ai.CtxKeyWriter, streamWriter)

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

	// Detect Database Switch
	// if payload.CurrentDB != "" && payload.CurrentDB != req.Database {
	// 	log.Info("Agent switched database", "from", req.Database, "to", payload.CurrentDB)
	// 	sendEvent("switch_database", payload.CurrentDB)
	// }

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

	if response != "" {
		sendEvent("content", response)
	}
}

func initAgents(ctx context.Context) {
	loadAgent(ctx, "copilot", "ai/data/copilot_pipeline.json")
}

func seedDefaultScripts(ctx context.Context, db *aidb.Database) {
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
	var existing ai.Script
	if err := store.Load(ctx, "general", "demo_loop", &existing); err == nil {
		tx.Rollback(ctx)
		return // Already exists
	}

	// Create demo_loop script
	demoLoop := ai.Script{
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

func loadAgent(ctx context.Context, key, configPath string) {
	// ctx is passed in

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

	// Apply Global config (LLM API Key, text embedders, etc)
	injectGlobalConfig(cfg, &config)

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
	sysOpts, err := getSystemDBOptions(ctx)
	var sysDB *aidb.Database
	if err == nil {
		pathExists := true
		if len(sysOpts.StoresFolders) > 0 {
			if _, statErr := os.Stat(sysOpts.StoresFolders[0]); os.IsNotExist(statErr) {
				pathExists = false
			}
		}
		if pathExists {
			sysDB = aidb.NewDatabase(sysOpts)
			// Seed default scripts for testing
			seedDefaultScripts(ctx, sysDB)
			seedSOPKnowledge(ctx, sysDB)
		} else {
			log.Warn("System DB path does not exist. Skipping AI Agent seeding to prevent auto-creation.")
		}
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
		opts, err := getDBOptionsFromConfig(ctx, &d)
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
		svc, err := agent.NewFromConfig(ctx, agentCfg, agent.Dependencies{
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
			// However, we can make the Generator itself memory!

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
	mainAgent, err := agent.NewFromConfig(ctx, *cfg, agent.Dependencies{
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
	// For now, we assume tools are handled by the "copilot" agent (CopilotAgent)
	// In a real system, we might look up the tool in a global registry or iterate agents.

	// Try copilot first
	if agentSvc, ok := e.Agents["copilot"]; ok {
		if da, ok := agentSvc.(*agent.CopilotAgent); ok {
			return da.Execute(ctx, tool, args)
		}
	}

	return "", fmt.Errorf("tool '%s' not found or no executor available", tool)
}

func injectGlobalConfig(cfg *agent.Config, globalCfg *Config) {
	if cfg == nil || globalCfg == nil {
		return
	}

	// 1. Brain Config
	if globalCfg.BrainProvider != "" {
		cfg.Generator.Type = globalCfg.BrainProvider
		if cfg.Generator.Options == nil {
			cfg.Generator.Options = make(map[string]any)
		}
		if globalCfg.BrainModel != "" {
			cfg.Generator.Options["model"] = globalCfg.BrainModel
		}
		if globalCfg.BrainURL != "" {
			cfg.Generator.Options["base_url"] = globalCfg.BrainURL
		}
		if globalCfg.BrainAPIKey != "" {
			cfg.Generator.Options["api_key"] = globalCfg.BrainAPIKey
		}
	} else if cfg.Generator.Type == "gemini" && globalCfg.LLMApiKey != "" {
		if cfg.Generator.Options == nil {
			cfg.Generator.Options = make(map[string]any)
		}
		// Only override if not set (or should we strictly override?)
		// Let's force override for now as the global config is likely the user's intent
		cfg.Generator.Options["api_key"] = globalCfg.LLMApiKey
	}

	// 2. Embedder Config
	if globalCfg.EmbedderProvider != "" {
		cfg.Embedder.Type = globalCfg.EmbedderProvider
		if cfg.Embedder.Options == nil {
			cfg.Embedder.Options = make(map[string]any)
		}
		if globalCfg.EmbedderModel != "" {
			cfg.Embedder.Options["model"] = globalCfg.EmbedderModel
		}
		if globalCfg.EmbedderURL != "" {
			cfg.Embedder.Options["base_url"] = globalCfg.EmbedderURL
		}
		if globalCfg.EmbedderAPIKey != "" {
			cfg.Embedder.Options["api_key"] = globalCfg.EmbedderAPIKey
		}
	} else {
		// Legacy Fallback
		if cfg.Embedder.Type == "gemini" && globalCfg.LLMApiKey != "" {
			if cfg.Embedder.Options == nil {
				cfg.Embedder.Options = make(map[string]any)
			}
			cfg.Embedder.Options["api_key"] = globalCfg.LLMApiKey
		} else if cfg.Embedder.Type == "ollama" {
			if globalCfg.OllamaEmbedderURL != "" || globalCfg.OllamaEmbedderModel != "" {
				if cfg.Embedder.Options == nil {
					cfg.Embedder.Options = make(map[string]any)
				}
				if globalCfg.OllamaEmbedderURL != "" {
					cfg.Embedder.Options["base_url"] = globalCfg.OllamaEmbedderURL
				}
				if globalCfg.OllamaEmbedderModel != "" {
					cfg.Embedder.Options["model"] = globalCfg.OllamaEmbedderModel
				}
			}
		}
	}
	// Recursive injection
	for i := range cfg.Agents {
		// We need to take address of slice element to modify it
		// But cfg.Agents is []Config.
		// wait, Config struct has []Config.
		// We can't easily recurse efficiently on value type slices in Go without pointers or rewriting the slice.
		// cfg.Agents is []Config (values).
		subCfg := &cfg.Agents[i]
		injectGlobalConfig(subCfg, globalCfg)
	}
}

func (e *DefaultToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}

func handleAIFeedback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		MsgID       string `json:"msgId"`
		Type        string `json:"type"` // positive, negative
		AIContent   string `json:"ai_content"`
		UserContent string `json:"user_content"`
		Database    string `json:"database"`
		Agent       string `json:"agent"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid body", http.StatusBadRequest)
		return
	}

	log.Info("AI Feedback Received", "msgId", req.MsgID, "type", req.Type)

	ctx := r.Context()

	// 1. Get System DB Options
	// We store feedback in the 'system' database to be global across user sessions/databases
	opts, err := getDBOptions(ctx, "system")
	if err != nil {
		log.Error("Failed to get system db options", "error", err)
		http.Error(w, "System DB error", http.StatusInternalServerError)
		return
	}

	// 2. Begin Transaction
	trans, err := database.BeginTransaction(ctx, opts, sop.ForWriting)
	if err != nil {
		log.Error("Failed to begin transaction", "error", err)
		http.Error(w, "DB trans error", http.StatusInternalServerError)
		return
	}
	// Ensure rollback if not committed
	defer trans.Rollback(ctx)

	// 3. Open Store "llm_feedback"
	storeName := "llm_feedback"
	// Simple string comparer
	comparer := func(a, b string) int { return strings.Compare(a, b) }

	// Configure store (auto-create if missing)
	so := sop.ConfigureStore(storeName, true, btree.DefaultSlotLength, "LLM Feedback Store", sop.SmallData, "")

	store, err := database.NewBtree[string, string](ctx, opts, storeName, trans, comparer, so)
	if err != nil {
		log.Error("Failed to open feedback store", "error", err)
		http.Error(w, "Store open error", http.StatusInternalServerError)
		return
	}

	// 4. Create Payload
	id := uuid.New().String()
	payload := map[string]any{
		// "id" removed - it is the Key
		"msg_id":       req.MsgID,
		"type":         req.Type,
		"ai_content":   req.AIContent,
		"user_content": req.UserContent,
		"database":     req.Database,
		"agent":        req.Agent,
		"timestamp":    time.Now().Format(time.RFC3339),
	}

	valBytes, _ := json.Marshal(payload)
	val := string(valBytes)

	// 5. Save (Add new record) to B-Tree
	if ok, err := store.Add(ctx, id, val); err != nil {
		log.Error("Failed to save feedback", "error", err)
		http.Error(w, "Save error", http.StatusInternalServerError)
		return
	} else if !ok {
		// Collision? Retry or just fail (UUID collision unlikely)
		http.Error(w, "Save failed (collision)", http.StatusInternalServerError)
		return
	}

	// 6. Vectorize & Save to Memory KnowledgeBase (if user content is present)
	if req.UserContent != "" {
		// Create Embedder (Simple Hash for now, ideally use Agent's Embedder)
		// 384 dimensions is good balance
		embedder := GetConfiguredEmbedder(nil)

		vecs, err := embedder.EmbedTexts(ctx, []string{req.UserContent})
		if err != nil {
			log.Warn("Failed to embed user content", "error", err)
			// Don't fail the request, just skip vectorization
		} else if len(vecs) > 0 {
			sysDB := aidb.NewDatabase(opts)
			kb, err := sysDB.OpenKnowledgeBase(ctx, "llm_feedback", trans, nil, embedder)
			if err != nil {
				log.Error("Failed to open physical knowledge base", "error", err)
				http.Error(w, "Knowledge base open error", http.StatusInternalServerError)
				return
			}

			if err := kb.IngestThought(ctx, req.UserContent, "Feedback", "System", vecs[0], map[string]any{"data": val}); err != nil {
				log.Error("Failed to ingest memory feedback", "error", err)
				http.Error(w, "Memory ingest error", http.StatusInternalServerError)
				return
			}
			log.Info("Feedback ingested to active memory", "id", id)
		}
	}

	// 7. Commit
	if err := trans.Commit(ctx); err != nil {
		log.Error("Failed to commit feedback", "error", err)
		http.Error(w, "Commit error", http.StatusInternalServerError)
		return
	}

	log.Info("Feedback Saved", "id", id)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "id": id})
}

// handleCloseSession allows explicitly closing an active session.
func handleCloseSession(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := r.URL.Query().Get("session_id")
	if sessionID == "" {
		http.Error(w, "query param session_id is required", http.StatusBadRequest)
		return
	}

	activeSessions.Close(sessionID)
	log.Info("Closed session explicitly", "session_id", sessionID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success", "message":"Session closed successfully"}`))
}

func handleToolExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Tool    string `json:"tool"`
		Text    string `json:"text"`
		Intent  string `json:"intent"`
		Context string `json:"context"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Invalid JSON body", "error", err)
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	agentSvc, exists := loadedAgents["copilot"]
	if !exists {
		http.Error(w, "Default AI agent not configured", http.StatusInternalServerError)
		return
	}

	switch req.Tool {
	case "enhance_text":
		prompt := fmt.Sprintf("Can you help me enhance this text for a %s?\n\n", strings.ReplaceAll(req.Intent, "_", " "))
		if req.Context != "" {
			prompt += fmt.Sprintf("Context / Details about the project:\n%s\n\n", req.Context)
		}
		prompt += fmt.Sprintf("Here is what I have so far:\n%s\n\nPlease improve it. Return ONLY the improved text, no conversational filler or markdown formatting.", req.Text)

		ctx := context.Background()

		if err := agentSvc.Open(ctx); err != nil {
			log.Error("Failed to open agent session", "error", err)
			http.Error(w, "Failed to initialize AI session", http.StatusInternalServerError)
			return
		}
		defer agentSvc.Close(ctx)

		response, err := agentSvc.Ask(ctx, prompt)
		if err != nil {
			log.Error("Failed to ask agent", "error", err)
			http.Error(w, "Failed to generate text", http.StatusInternalServerError)
			return
		}

		response = strings.TrimSpace(response)
		response = strings.TrimPrefix(response, "```xml")
		response = strings.TrimPrefix(response, "```markdown")
		response = strings.TrimPrefix(response, "```json")
		response = strings.TrimPrefix(response, "```")
		response = strings.TrimSuffix(response, "```")
		response = strings.TrimSpace(response)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"enhanced_text": response,
		})
	default:
		http.Error(w, "Unknown tool", http.StatusBadRequest)
	}
}
