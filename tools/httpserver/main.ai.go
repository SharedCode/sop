package main

import (
	"context"
	"encoding/json"
	"fmt"
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
)

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
		Format    string `json:"format"`
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Agent '%s' failed to open session: %v", req.Agent, err),
		})
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"error": fmt.Sprintf("Agent '%s' failed: %v", req.Agent, err),
		})
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
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"response": fmt.Sprintf("Agent attempted to call tool '%s' but failed to execute it internally.", toolCall.Tool),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"response": response,
	})
}

func initAgents() {
	loadAgent("sql_admin", "ai/data/sql_admin_pipeline.json")
}

func seedDefaultMacros(db *aidb.Database) {
	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to begin transaction for seeding macros: %v", err))
		return
	}
	store, err := db.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		log.Error(fmt.Sprintf("Failed to open macro store: %v", err))
		return
	}

	// Check if demo_loop exists
	// We force update it to ensure latest schema is used during development
	// var existing ai.Macro
	// if err := store.Load(ctx, "macros", "demo_loop", &existing); err == nil {
	// 	tx.Rollback(ctx)
	// 	return // Already exists
	// }

	// Create demo_loop macro
	demoLoop := ai.Macro{
		Name:        "demo_loop",
		Description: "Demonstrates loops and variables",
		Steps: []ai.MacroStep{
			{
				Type:     "set",
				Variable: "items",
				Value:    "apple\nbanana\ncherry",
			},
			{
				Type:     "loop",
				List:     "items",
				Iterator: "fruit",
				Steps: []ai.MacroStep{
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

	if err := store.Save(ctx, "macros", "demo_loop", demoLoop); err != nil {
		log.Error(fmt.Sprintf("Failed to save demo_loop macro: %v", err))
		tx.Rollback(ctx)
		return
	}

	tx.Commit(ctx)
	log.Info("Seeded 'demo_loop' macro.")
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

	// Initialize System DB
	sysOpts, err := getSystemDBOptions()
	var sysDB *aidb.Database
	if err == nil {
		sysDB = aidb.NewDatabase(sysOpts)
		// Seed default macros for testing
		seedDefaultMacros(sysDB)
	} else {
		log.Debug(fmt.Sprintf("System DB not available for agent %s: %v", cfg.ID, err))
	}

	// Prepare Databases map
	databases := make(map[string]sop.DatabaseOptions)
	for _, dbCfg := range config.Databases {
		// We need to use a copy of dbCfg because getDBOptionsFromConfig takes a pointer
		// and loop variable reuse might be an issue in older Go versions, though fixed in 1.22.
		// Safe to just pass address.
		d := dbCfg
		opts, err := getDBOptionsFromConfig(&d)
		if err == nil {
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
		return agent.NewFromConfig(context.Background(), agentCfg, agent.Dependencies{
			AgentRegistry: registry,
			SystemDB:      sysDB,
			Databases:     databases,
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
			return da.ExecuteTool(ctx, tool, args)
		}
	}

	return "", fmt.Errorf("tool '%s' not found or no executor available", tool)
}

func (e *DefaultToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}
