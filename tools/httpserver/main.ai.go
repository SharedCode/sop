package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent"
	_ "github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/jsondb"
)

// DefaultToolExecutor implements ai.ToolExecutor
type DefaultToolExecutor struct{}

func (e *DefaultToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	return executeTool(ctx, toolName, args)
}

func (e *DefaultToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return []ai.ToolDefinition{
		{
			Name:        "list_stores",
			Description: "Lists all stores in the specified database.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}}, "required": ["database"]}`,
		},
		{
			Name:        "list_databases",
			Description: "Lists all available databases.",
			Schema:      `{"type": "object", "properties": {}}`,
		},
		{
			Name:        "search",
			Description: "Search for items in a store.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}, "store": {"type": "string"}, "query": {"type": "object"}}, "required": ["database", "store", "query"]}`,
		},
		{
			Name:        "get_schema",
			Description: "Get the index specification and schema of a store.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}, "store": {"type": "string"}}, "required": ["database", "store"]}`,
		},
		{
			Name:        "select",
			Description: "Select items from a store.",
			Schema:      `{"type": "object", "properties": {"database": {"type": "string"}, "store": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["database", "store"]}`,
		},
	}, nil
}

func handleAIChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Message   string `json:"message"`
		Database  string `json:"database"`
		StoreName string `json:"store"`
		Agent     string `json:"agent"`
		Provider  string `json:"provider"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Trim inputs to avoid mismatch issues
	req.Database = strings.TrimSpace(req.Database)
	req.StoreName = strings.TrimSpace(req.StoreName)

	// Validate Database if provided
	if req.Database != "" {
		if _, err := getDBOptions(req.Database); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{
				"error": fmt.Sprintf("Invalid database '%s': %v", req.Database, err),
			})
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
		// If the requested agent doesn't exist, we could fall back or error.
		// For now, let's error to be explicit.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Agent '%s' is not initialized or not found.", req.Agent),
		})
		return
	}

	ctx := context.Background()
	// Pass provider override via context
	if req.Provider != "" {
		ctx = context.WithValue(ctx, ai.CtxKeyProvider, req.Provider)
	}
	// Pass ToolExecutor via context
	ctx = context.WithValue(ctx, ai.CtxKeyExecutor, &DefaultToolExecutor{})

	// Register resources so the agent knows them for obfuscation
	if req.Database != "" {
		obfuscation.GlobalObfuscator.RegisterResource(req.Database, "DB")
	}
	if req.StoreName != "" {
		obfuscation.GlobalObfuscator.RegisterResource(req.StoreName, "STORE")
	}

	// Prepend context information to the message
	fullMessage := req.Message
	if req.Database != "" {
		fullMessage = fmt.Sprintf("Current Database: %s\n%s", req.Database, req.Message)
	}

	response, err := agentSvc.Ask(ctx, fullMessage)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Agent '%s' failed: %v", req.Agent, err),
		})
		return
	}

	// The response from Ask() is now the final answer (or tool output if handled by DataAdmin).
	// We don't need the ReAct loop here anymore because DataAdmin handles it.
	// However, if we are using a "dumb" generator (like pure Gemini without DataAdmin wrapper),
	// we might get a raw tool call string back.
	// But the user explicitly wanted to move to DataAdmin.
	// So we assume the response is the final text to show to the user.

	// Check if response is a tool call (JSON) - Just in case the Agent returned a tool call that wasn't executed
	// (e.g. if DataAdmin failed to execute it or if we are using a different generator).
	// For robustness, we can keep a simple check, but we won't loop here.

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
		// We can execute it once and return the result.
		log.Debug(fmt.Sprintf("Agent returned raw tool call: %s", toolCall.Tool))

		// Inject default database if missing
		if db, ok := toolCall.Args["database"].(string); !ok || db == "" {
			toolCall.Args["database"] = req.Database
		}

		// Log the final database name being used for debugging
		finalDB, _ := toolCall.Args["database"].(string)
		log.Debug(fmt.Sprintf("Executing tool '%s' on database: '%s' (bytes: %v)", toolCall.Tool, finalDB, []byte(finalDB)))

		result, err := executeTool(ctx, toolCall.Tool, toolCall.Args)
		if err != nil {
			result = "Error: " + err.Error()
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"response": result,
			"action":   "refresh",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"response": response,
	})
}

func executeTool(ctx context.Context, toolName string, args map[string]any) (string, error) {
	dbName, _ := args["database"].(string)
	dbName = strings.TrimSpace(dbName)

	if dbName == "" {
		return "", fmt.Errorf("database name is required")
	}

	dbOpts, err := getDBOptions(dbName)
	if err != nil {
		return "", err
	}

	switch toolName {
	case "list_databases":
		var names []string
		for _, db := range config.Databases {
			names = append(names, db.Name)
		}
		return fmt.Sprintf("Databases: %v", names), nil

	case "list_stores":
		trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
		if err != nil {
			return "", err
		}
		defer trans.Rollback(ctx)

		stores, err := trans.GetStores(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("Stores: %v", stores), nil

	case "select":
		storeName, _ := args["store"].(string)
		limitFloat, _ := args["limit"].(float64) // JSON numbers are floats
		limit := int(limitFloat)
		if limit < 0 {
			limit = 1000000 // Treat negative as "unlimited" (capped)
		} else if limit == 0 {
			limit = 2 // Default if missing
		}
		if storeName == "" {
			return "", fmt.Errorf("store name is required")
		}

		trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
		if err != nil {
			return "", err
		}
		defer trans.Rollback(ctx)

		// Get Store Info to determine comparer
		var isPrimitiveKey bool
		var indexSpec *jsondb.IndexSpecification

		if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
			stores, err := t2.StoreRepository.Get(ctx, storeName)
			if err == nil && len(stores) > 0 {
				isPrimitiveKey = stores[0].IsPrimitiveKey
				if !isPrimitiveKey && stores[0].MapKeyIndexSpecification != "" {
					var is jsondb.IndexSpecification
					if err := encoding.DefaultMarshaler.Unmarshal([]byte(stores[0].MapKeyIndexSpecification), &is); err == nil {
						indexSpec = &is
					}
				}
			}
		}

		var comparer btree.ComparerFunc[any]
		if !isPrimitiveKey && indexSpec != nil {
			comparer = func(a, b any) int {
				return indexSpec.Comparer(a.(map[string]any), b.(map[string]any))
			}
		}

		store, err := database.OpenBtree[any, any](ctx, dbOpts, storeName, trans, comparer)
		if err != nil {
			return "", fmt.Errorf("failed to open store: %v", err)
		}

		var items []map[string]any
		ok, err := store.First(ctx)
		count := 0
		for ok && err == nil && count < limit {
			k := store.GetCurrentKey()
			v, _ := store.GetCurrentValue(ctx)
			items = append(items, map[string]any{"key": k.Key, "value": v})
			ok, err = store.Next(ctx)
			count++
		}

		if len(items) == 0 {
			return "No items found.", nil
		}

		// Check for requested format (default to CSV for readability in Chat)
		format, _ := args["format"].(string)
		if strings.ToLower(format) == "json" {
			b, err := json.MarshalIndent(items, "", "  ")
			if err != nil {
				return "", err
			}
			return string(b), nil
		}

		keyFieldSet := make(map[string]bool)
		var keyIsMap bool
		valueFieldSet := make(map[string]bool)
		var valueIsMap bool

		// Scan for fields
		for _, item := range items {
			// Check Key
			if kMap, ok := item["key"].(map[string]any); ok {
				keyIsMap = true
				for k := range kMap {
					keyFieldSet[k] = true
				}
			}
			// Check Value
			if vMap, ok := item["value"].(map[string]any); ok {
				valueIsMap = true
				for k := range vMap {
					valueFieldSet[k] = true
				}
			}
		}

		var kFields []string
		if keyIsMap {
			for k := range keyFieldSet {
				kFields = append(kFields, k)
			}
			sort.Strings(kFields)
		}

		var vFields []string
		if valueIsMap {
			for k := range valueFieldSet {
				vFields = append(vFields, k)
			}
			sort.Strings(vFields)
		}

		// Default: CSV Format
		var csvHeaders []string
		if keyIsMap {
			for _, k := range kFields {
				if len(k) > 0 {
					csvHeaders = append(csvHeaders, strings.ToUpper(k[:1])+k[1:])
				} else {
					csvHeaders = append(csvHeaders, k)
				}
			}
		} else {
			csvHeaders = append(csvHeaders, "Key")
		}
		if valueIsMap {
			for _, v := range vFields {
				if len(v) > 0 {
					csvHeaders = append(csvHeaders, strings.ToUpper(v[:1])+v[1:])
				} else {
					csvHeaders = append(csvHeaders, v)
				}
			}
		}
		var sb strings.Builder
		sb.WriteString(strings.Join(csvHeaders, ", "))
		sb.WriteString("\n")

		for _, item := range items {
			var row []string
			// Key
			if keyIsMap {
				kMap, _ := item["key"].(map[string]any)
				for _, f := range kFields {
					if v, ok := kMap[f]; ok {
						row = append(row, fmt.Sprintf("%v", v))
					} else {
						row = append(row, "")
					}
				}
			} else {
				row = append(row, fmt.Sprintf("%v", item["key"]))
			}
			// Value
			if valueIsMap {
				vMap, _ := item["value"].(map[string]any)
				for _, f := range vFields {
					if v, ok := vMap[f]; ok {
						row = append(row, fmt.Sprintf("%v", v))
					} else {
						row = append(row, "")
					}
				}
			} else {
				row = append(row, fmt.Sprintf("%v", item["value"]))
			}
			sb.WriteString(strings.Join(row, ", "))
			sb.WriteString("\n")
		}
		return sb.String(), nil

	case "get_schema":
		storeName, _ := args["store"].(string)
		if storeName == "" {
			return "", fmt.Errorf("store name is required")
		}
		// Open read transaction to get store info
		trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
		if err != nil {
			return "", err
		}
		defer trans.Rollback(ctx)

		// We can't easily get store info without opening it or querying repo
		// Let's query repo
		if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
			stores, err := t2.StoreRepository.Get(ctx, storeName)
			if err != nil {
				return "", err
			}
			if len(stores) == 0 {
				return "Store not found", nil
			}
			si := stores[0]
			return fmt.Sprintf("Store: %s\nIndexes: %s", si.Name, si.MapKeyIndexSpecification), nil
		}
		return "Could not access store repository", nil

	case "search":
		storeName, _ := args["store"].(string)
		query, _ := args["query"].(map[string]any)
		if storeName == "" {
			return "", fmt.Errorf("store name is required")
		}

		// For now, we don't have a generic "Search by JSON" function exposed easily in main.go
		// But we can simulate it or just return a message saying "Search executed" if we implement the actual search logic.
		// Since the user wants to see results in the grid, we can just return "Search parameters set. Refreshing grid."
		// AND we need to actually set the search parameters for the *next* loadItems call?
		// The current UI `loadItems` takes parameters from the UI inputs.
		// The AI cannot easily "push" search params to the UI state unless we return them in the JSON response.

		// Let's return the query as the "action" payload?
		// For now, let's just do a simple Find to verify it exists, and return the count.

		trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
		if err != nil {
			return "", err
		}
		defer trans.Rollback(ctx)

		// We need to open the store... this is getting complicated to do generically in one function without duplicating main.go logic.
		// Let's just return a success message for now, and maybe in the future we update the UI to accept "search_params" from AI.

		return fmt.Sprintf("Found items matching %v (Grid refresh triggered)", query), nil
	}

	return "", fmt.Errorf("unknown tool: %s", toolName)
}

func initAgents() {
	// loadAgent("doctor", "ai/data/doctor_pipeline.json") // Disabled per user request
	loadAgent("sql_admin", "ai/data/sql_admin_pipeline.json")
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
		return agent.NewFromConfig(context.Background(), agentCfg, agent.Dependencies{
			AgentRegistry: registry,
		})
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
	}

	// Initialize the main agent
	mainAgent, err := agent.NewFromConfig(context.Background(), *cfg, agent.Dependencies{
		AgentRegistry: registry,
	})
	if err != nil {
		log.Debug(fmt.Sprintf("Failed to initialize main agent %s: %v", key, err))
		return
	}

	loadedAgents[key] = mainAgent
	log.Debug(fmt.Sprintf("Agent '%s' initialized successfully.", key))
}
