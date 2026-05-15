package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/agent/parser"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/memory"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/jsondb"
)

const (
	MaxMRUSize            = 20
	MaxExchangesInHistory = 20
)

// MRUItem represents a single category currently in working memory
type MRUItem struct {
	Category     string
	LastAccessed int64
	Context      string
}

// MemoryUnit encapsulates the cognitive state and boundaries of an Agent instance.
type MemoryUnit struct {
	AgentID    string
	AllowedKBs []string // LTM scoping boundaries

	// Physical On-Disk Memory Structures
	STM btree.BtreeInterface[string, any]     // Episodic B-Tree buffer for the cognitive footprint
	LTM *memory.KnowledgeBase[map[string]any] // Declarative Vector Database (Sellable knowledge mapping)
}

// CopilotAgent is a specialized agent for database administration tasks.
// It implements the ai.Agent interface.
type CopilotAgent struct {
	Config       Config
	brain        ai.Generator
	registry     *Registry
	databases    map[string]sop.DatabaseOptions
	systemDB     *database.Database
	lastToolCall *ai.ScriptStep
	service      *Service // Reference back to main service for cache invalidation

	// Encapsulated memory and context boundaries
	Memory        *MemoryUnit
	episodeQueue  chan map[string]any // Agent-scoped STM batching queue
	lastEpisodeTS atomic.Int64        // Tracks the last time an episode was logged to STM for idle sleep cycles

	// Compiled Scripts Cache
	compiledScripts   map[string]CachedScript
	compiledScriptsMu sync.RWMutex

	// API Keys for dynamic switching
	geminiKey string
	openAIKey string

	// StoreOpener allows mocking the store creation (e.g. for testing)
	StoreOpener func(ctx context.Context, dbOpts sop.DatabaseOptions, storeName string, tx sop.Transaction) (jsondb.StoreAccessor, error)
}

// Clone creates a new isolated instance of the agent sharing read-only components.
func (a *CopilotAgent) Clone() ai.Agent[map[string]any] {
	return &CopilotAgent{
		Config:    a.Config,
		brain:     a.brain,
		registry:  a.registry, // Pointer to registry
		databases: a.databases,
		systemDB:  a.systemDB,
		Memory: &MemoryUnit{
			AgentID: "omni", // By default Clone behaves as Omni, Avatars explicitly override
		},
		lastToolCall:    nil,
		service:         nil, // Caller should populate this
		compiledScripts: make(map[string]CachedScript),
		geminiKey:       a.geminiKey,
		openAIKey:       a.openAIKey,
		StoreOpener:     a.StoreOpener,
	}
}

// SetGenerator sets the generator for the agent.
func (a *CopilotAgent) SetGenerator(gen ai.Generator) {
	a.brain = gen
}

// MarkMRUCategory adds or updates a category in the global working memory MRU
func (a *CopilotAgent) MarkMRUCategory(category string, context string) {
	if a.service == nil || a.service.session == nil {
		return
	}
	sess := a.service.session
	sess.MRUMu.Lock()
	defer sess.MRUMu.Unlock()

	ts := time.Now().UnixMilli()

	// Update if exists
	for i, item := range sess.MRU {
		if item.Category == category {
			sess.MRU[i].LastAccessed = ts
			if context != "" {
				sess.MRU[i].Context = context
			}
			return
		}
	}

	// Add new
	sess.MRU = append(sess.MRU, MRUItem{
		Category:     category,
		LastAccessed: ts,
		Context:      context,
	})

	// Sort by newest and shrink if > MaxMRUSize
	if len(sess.MRU) > MaxMRUSize {
		sort.Slice(sess.MRU, func(i, j int) bool {
			return sess.MRU[i].LastAccessed > sess.MRU[j].LastAccessed
		})
		sess.MRU = sess.MRU[:MaxMRUSize]
	}
}

// NewCopilotAgent creates a new instance of CopilotAgent.
func NewCopilotAgent(cfg Config, databases map[string]sop.DatabaseOptions, systemDB *database.Database) *CopilotAgent {
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

	agent := &CopilotAgent{
		Config:    cfg,
		brain:     gen,
		registry:  NewRegistry(),
		databases: databases,
		systemDB:  systemDB,
		Memory: &MemoryUnit{
			AgentID: "omni", // Root loop defaults to omni
		},
		// API Keys are less relevant now that we use the Generator interface mostly,
		// but we keep them empty or fill them if we really need them for direct calls later.
		// geminiKey:       geminiKey,
		// openAIKey:       openAIKey,
		compiledScripts: make(map[string]CachedScript),
		episodeQueue:    make(chan map[string]any, 100),
	}
	// Tools are registered dynamically in Open() or Ask() to ensure context propagation
	// context.Background() is used here to ensure tools are available even if Open() is not called or context context is missing
	agent.registerTools(context.Background())

	return agent
}

// SetService sets the reference to the main service (for cache invalidation).
func (a *CopilotAgent) SetService(s *Service) {
	a.service = s
}

// shouldObfuscate determines if obfuscation should be applied for a given database.
func (a *CopilotAgent) shouldObfuscate(dbName string) bool {
	// Look up database options
	if opts, ok := a.databases[dbName]; ok {
		return opts.EnableObfuscation
	}
	// Fallback to mode if no DB options found (e.g. legacy or system)
	// But usually systemDB is also in databases map.
	return false
}

// SetVerbose enables or disables verbose output.
func (a *CopilotAgent) SetVerbose(v bool) {
	a.Config.Verbose = v
}

// Open initializes the agent's resources.
func (a *CopilotAgent) Open(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)

	// If CurrentDB is set, start a transaction
	if p != nil && p.CurrentDB != "" {
		dbName := p.CurrentDB

		log.Debug(fmt.Sprintf("CopilotAgent.Open: Checking DB '%s', SystemDB available: %v", dbName, a.systemDB != nil))

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

	// Initialize strict physical cognitive buffers natively
	if err := a.InitializePhysicalMemory(ctx); err != nil {
		log.Warn("CopilotAgent: STM Initialization failure", "error", err)
	}

	// Register tools now that we have a context (and potentially transactions)
	a.registerTools(ctx)

	return nil
}

// Close cleans up the agent's resources.
func (a *CopilotAgent) Close(ctx context.Context) error {
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
func (a *CopilotAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return []ai.Hit[map[string]any]{}, nil
}

// Ask processes a query and returns a response.
func (a *CopilotAgent) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	// 1. Determine Generator to use (Dynamic Switching)
	gen := a.resolveGenerator(ctx)

	// 2. Handle Direct Tool Invocations (Slash Commands)
	if strings.HasPrefix(strings.TrimSpace(query), "/") {
		// Register tools for local command parsing
		a.registerTools(ctx)
		handled, res, err := a.handleSlashCommand(ctx, query, gen)
		if handled {
			if err != nil {
				return res, nil // Return the error message string directly as per original flow
			}
			return res, nil
		}
	}

	if gen == nil {
		return "⚠️ **AI Copilot Disabled**: No valid API Key found.\n\nPlease go to **Environment Settings** (HDD icon in bottom left) -> **LLM API Key** to configure your Google Gemini or OpenAI key.", nil
	}

	// 3. Classify Intent Router (OMNI vs specific Avatar)
	intent := a.classifyIntent(ctx, query, gen)

	// 4. Fast-path routing: If Avatar, execute Avatar Sub-Agent
	if intent != "OMNI" {
		log.Info("Ask: Request classified for Avatar routing", "avatar", intent)
		return a.executeAvatarSubAgent(ctx, intent, query)
	}

	// 5. Omni routing: Load heavy baseline tools, compile system prompt, execute
	log.Info("Ask: Request classified for OMNI baseline")

	// Determine current target KB
	currentKBTrack := "sop"
	if p := ai.GetSessionPayload(ctx); p != nil && len(p.SelectedKBs) > 0 {
		currentKBTrack = strings.Join(p.SelectedKBs, ",")
	}

	// --- DYNAMIC MRU INJECTION FROM PREVIOUS EPISODE ---
	if a.service != nil && a.service.session != nil {
		if a.service.session.Memory != nil {
			thread := a.service.session.Memory.GetCurrentThread()
			if thread != nil && len(thread.Exchanges) > 0 {
				lastExchange := thread.Exchanges[len(thread.Exchanges)-1]

				// Inspect the prior Q&A exchange (episode metadata)
				// If the interaction resolves to the same Entity and same Knowledge Base...
				if lastExchange.Entity == intent && lastExchange.ActiveKB == currentKBTrack {
					// Auto-inject the matching contexts into MRU
					kbs := strings.Split(currentKBTrack, ",")
					for _, kb := range kbs {
						kb = strings.TrimSpace(kb)
						if kb != "" {
							a.MarkMRUCategory(kb, "")
						}
					}
				}
			}
		}
	}

	a.registerTools(ctx)

	// 6. Construct System Prompt with Persona & Tools & Context
	fullPrompt := a.buildSystemPrompt(ctx, query)

	// Obfuscate Prompt if enabled
	currentDB := ""
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDB = p.CurrentDB
	}
	if a.shouldObfuscate(currentDB) {
		fullPrompt = obfuscation.GlobalObfuscator.ObfuscateText(fullPrompt)
	}

	// 5. Delegate to Reasoning Engine

	var engine ai.ReasoningEngine
	if a.Config.UseLegacyBaselineEngine {
		engine = &BaselineReActEngine{
			Agent: a,
		}
	} else {
		// Active Implementation: Native Tools (API-level tool calling)
		engine = &NativeReActEngine{
			EnableObfuscation: a.shouldObfuscate(currentDB),
		}
	}

	req := ai.ReasoningRequest{
		SystemPrompt: fullPrompt, // For baseline, the system prompt contains the full aggregated state
		UserQuery:    query,
		Executor:     a, // CopilotAgent implements Executor
		Generator:    gen,
		// ContextText and HistoryText are pre-injected into fullPrompt in this legacy baseline
	}

	resp, err := engine.Run(ctx, req)
	if err != nil {
		return "", err
	}

	// 6. Post-Processing
	finalText := resp.FinalText
	if a.shouldObfuscate(currentDB) {
		finalText = obfuscation.GlobalObfuscator.DeobfuscateText(finalText)
	}

	if a.service != nil && a.service.session != nil {
		if a.service.session.Memory == nil {
			a.service.session.Memory = NewShortTermMemory()
		}
		thread := a.service.session.Memory.GetCurrentThread()
		if thread == nil {
			newThreadID := sop.NewUUID()
			thread = &ConversationThread{
				ID:         newThreadID,
				RootPrompt: query,
				Label:      "Copilot Session",
				Category:   "General",
				Exchanges:  make([]Interaction, 0),
				Status:     "active",
			}
			a.service.session.Memory.AddThread(thread)
			a.service.session.Memory.CurrentThreadID = newThreadID
		}

		kbTrack := "sop"
		if p := ai.GetSessionPayload(ctx); p != nil && len(p.SelectedKBs) > 0 {
			kbTrack = strings.Join(p.SelectedKBs, ",")
		}

		// Track User
		thread.Exchanges = append(thread.Exchanges, Interaction{
			Role:      RoleUser,
			Content:   query,
			Timestamp: time.Now().UnixMilli(),
			Entity:    intent,
			ActiveKB:  kbTrack,
		})
		// Track Assistant
		thread.Exchanges = append(thread.Exchanges, Interaction{
			Role:      RoleAssistant,
			Content:   finalText,
			Timestamp: time.Now().UnixMilli(),
			Entity:    intent,
			ActiveKB:  kbTrack,
		})

	}

	// 7. Log Episode for SleepCycle (Episodic Memory)
	if a.service != nil && a.service.EnableShortTermMemory {
		var mruSnapshot []MRUItem
		if a.service.session != nil {
			a.service.session.MRUMu.RLock()
			mruSnapshot = append([]MRUItem(nil), a.service.session.MRU...)
			a.service.session.MRUMu.RUnlock()
		}

		thoughtPayload := map[string]any{
			"query":          query,
			"response":       finalText,
			"active_context": mruSnapshot,
		}

		go a.logEpisodeToSTM(context.Background(), "user_interaction", thoughtPayload, "Interacted with user", nil)
	}

	return finalText, nil
}

// ----------------------------------------------------------------------------
// HELPER METHODS
// ----------------------------------------------------------------------------

func (a *CopilotAgent) resolveGenerator(ctx context.Context) ai.Generator {
	gen := a.brain
	providerOverride, ok := ctx.Value(ai.CtxKeyProvider).(string)
	if !ok || providerOverride == "" {
		return gen
	}

	var err error
	var tempGen ai.Generator

	customAPIKey, _ := ctx.Value(ai.CtxKeyAPIKey).(string)
	customBaseURL, _ := ctx.Value(ai.CtxKeyBaseURL).(string)

	switch providerOverride {
	case ProviderGemini:
		key := customAPIKey
		if key == "" {
			key = a.geminiKey
		}
		if key != "" {
			model := os.Getenv(EnvGeminiModel)
			if model == "" {
				model = DefaultModelGemini
			}
			tempGen, err = generator.New(ProviderGemini, map[string]any{
				"api_key": key,
				"model":   model,
			})
		}
	case ProviderChatGPT:
		key := customAPIKey
		if key == "" {
			key = a.openAIKey
		}
		if key != "" {
			model := customBaseURL
			if model == "" {
				model = os.Getenv(EnvOpenAIModel)
			}
			if model == "" {
				model = DefaultModelOpenAI
			}
			tempGen, err = generator.New(ProviderChatGPT, map[string]any{
				"api_key": key,
				"model":   model,
			})
		}
	case ProviderOllama:
		model := os.Getenv(EnvOllamaModel)
		if model == "" {
			model = DefaultModelOllama
		}
		host := customBaseURL
		if host == "" {
			host = os.Getenv(EnvOllamaHost)
		}
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

	return gen
}

func (a *CopilotAgent) handleSlashCommand(ctx context.Context, query string, gen ai.Generator) (bool, string, error) {
	cmdLine := strings.TrimSpace(query)[1:]
	toolName, args, err := parser.ParseSlashCommand(cmdLine)

	if err == nil && (toolName == "verbose" || toolName == "v") {
		newState := !a.Config.Verbose
		if pos, ok := args["_positional"].([]string); ok && len(pos) > 0 {
			val := strings.ToLower(pos[0])
			if val == "on" || val == "true" || val == "1" {
				newState = true
			} else if val == "off" || val == "false" || val == "0" {
				newState = false
			}
		}
		a.SetVerbose(newState)
		status := "OFF"
		if newState {
			status = "ON"
		}
		return true, fmt.Sprintf("Verbose mode: **%s**", status), nil
	}

	if err == nil && toolName != "" {
		res, execErr := a.Execute(ctx, toolName, args)
		if execErr != nil {
			if strings.Contains(execErr.Error(), "unknown tool") {
				if gen != nil {
					log.Warn("Slash command failed locally (unknown tool), falling back to LLM", "tool", toolName, "error", execErr)
					return false, "", nil // Handled by LLM
				}
				return true, fmt.Sprintf("Error executing command '%s' (and no AI Copilot available to interpret it): %v", toolName, execErr), fmt.Errorf("tool fallback failed")
			}
			return true, fmt.Sprintf("Error executing command '%s': %v", toolName, execErr), fmt.Errorf("tool execution failed")
		}
		return true, res, nil
	}
	return false, "", nil
}

func (a *CopilotAgent) buildSystemPrompt(ctx context.Context, query string) string {
	persona := ""
	if p := ai.GetSessionPayload(ctx); p != nil && p.UserID != "" && a.systemDB != nil {
		kbName := p.GetMemoryKBName()
		if tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading); err == nil {
			if kb, err := a.systemDB.OpenKnowledgeBase(ctx, kbName, tx, nil, nil); err == nil {
				if cfg, err := kb.GetConfig(ctx); err == nil && cfg != nil && cfg.SystemPrompt != "" {
					persona = cfg.SystemPrompt + "\n\n"
				}
			}
			tx.Rollback(ctx)
		}
	}

	// Fallback persona
	if persona == "" {
		persona = "You are a general-purpose intelligent AI Copilot equipped with a human-like 'active memory' system. " +
			"You specialize in SOP AI (SOP for AI), which is primarily a platform and toolset for Knowledge Bases (KBs). " +
			"SOP allows users to author KBs, and you can be instructed to consult all KBs in the current DB and/or SystemDB to provide informed answers. " +
			"You also aid in SOP library adoption, technology integration, Spaces management, and data management. " +
			"As a true SOP (Scalable Objects Persistence) expert, your core knowledge covers Databases, B-Trees, strict ACID Transactions, Swarm Computing, and advanced Storage mechanisms including Erasure Coding. " +
			"You also understand that a 'Space' or 'Knowledge Base' is a new AI memory subsystem combining VectorDB, Text Search, and a specialized schema (Thoughts: Category/Items), and you manage it differently than raw technical tables. " +
			"You have deep expertise in SOP scripting (AST-based execution), and the SOP HTTP API, covering request/response lifecycles, NDJSON streaming, and session management. " +
			"You derive your foundational SOP knowledge, codebase context, and architectural principles directly from the source repository at https://github.com/sharedcode/sop. " +
			"Assist users dynamically with ANY open-ended request—whether answering general questions, creating and consulting Knowledge Bases, writing code, or managing database queries using the tools provided.\n\n"
	}

	toolsDef := persona
	isSpaceGeneration := strings.Contains(query, "IMPORTANT SYSTEM RULE: The user is generating a UI Space.")

	if !isSpaceGeneration {
		toolsDef += a.registry.GeneratePrompt()
	}

	// Append Scripts as Tools
	if a.systemDB != nil && !isSpaceGeneration {
		if tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading); err == nil {
			if store, err := a.systemDB.OpenModelStore(ctx, "scripts", tx); err == nil {
				if names, err := store.List(ctx, ai.DefaultScriptCategory); err == nil {
					for _, name := range names {
						var script ai.Script
						if err := store.Load(ctx, ai.DefaultScriptCategory, name, &script); err == nil {
							argsSchema := "()"
							if len(script.Parameters) > 0 {
								var params []string
								for _, p := range script.Parameters {
									params = append(params, fmt.Sprintf("%s: string", p))
								}
								argsSchema = fmt.Sprintf("(%s)", strings.Join(params, ", "))
							}
							toolsDef += fmt.Sprintf("- %s: %s %s\n", name, script.Description, argsSchema)
						}
					}
				}
			}
			tx.Commit(ctx)
		}
	}

	// Contextual Learned Knowledge (JIT Semantic Recall from Episodic LTM)
	if a.systemDB != nil && a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil && a.Memory.AgentID != "" {
		if tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading); err == nil {
			kbName := fmt.Sprintf("ltm_%s", a.Memory.AgentID)
			kb, err := a.systemDB.OpenKnowledgeBase(ctx, kbName, tx, nil, nil)
			if err == nil {
				vecs, err := a.service.Domain().Embedder().EmbedTexts(ctx, []string{query})
				if err == nil && len(vecs) > 0 {
					closestCat, _, err := kb.Manager.FindClosestCategory(ctx, vecs[0])
					catFilter := ""
					if err == nil && closestCat != nil {
						catFilter = closestCat.Name
					}
					hits, err := kb.SearchSemantics(ctx, vecs[0], &memory.SearchOptions[map[string]any]{Limit: 5, Category: catFilter})
					if err == nil && len(hits) > 0 {
						hasKnowledge := false
						knowledgeStr := ""
						for _, hit := range hits {
							if hit.Score < 0.6 {
								continue
							}
							valStr := ""
							if str, ok := hit.Payload["_raw_content"].(string); ok {
								valStr = str
							} else if str, ok := hit.Payload["content"].(string); ok {
								valStr = str
							} else {
								valStr = fmt.Sprintf("%v", hit.Payload)
							}
							if valStr != "" {
								hasKnowledge = true
								knowledgeStr += fmt.Sprintf("- (Score: %.2f) %s\n", hit.Score, valStr)
							}
						}
						if hasKnowledge {
							toolsDef += "\nContext Section (Learned Knowledge):\n" + knowledgeStr
						}
					}
				}
			}
			tx.Rollback(ctx)
		}
	}

	// Active Domains Lookup
	if p := ai.GetSessionPayload(ctx); p != nil && p.ActiveDomain != "" {
		domains := strings.Split(p.ActiveDomain, ",")
		for _, domain := range domains {
			domain = strings.TrimSpace(domain)
			if domain == "" || domain == "custom" {
				continue
			}

			var domainDB *database.Database
			dbOptsList := []sop.DatabaseOptions{a.systemDB.Config()}
			for _, dbOpts := range a.databases {
				dbOptsList = append(dbOptsList, dbOpts)
			}

			for _, dbOpts := range dbOptsList {
				tempDB := database.NewDatabase(dbOpts)
				kbs := func(ctx context.Context, tempDB *database.Database) []string {
					var kbs []string
					if tx, err := tempDB.BeginTransaction(ctx, sop.ForReading); err == nil {
						if stores, err := tx.GetStores(ctx); err == nil {
							for _, s := range stores {
								if strings.HasSuffix(s, "/sys_config") {
									kbs = append(kbs, strings.TrimSuffix(s, "/sys_config"))
								}
							}
						}
						tx.Rollback(ctx)
					}
					return kbs
				}(ctx, tempDB)

				hasDomain := false
				for _, kb := range kbs {
					if kb == domain {
						hasDomain = true
						break
					}
				}
				if hasDomain {
					domainDB = tempDB
					break
				}
			}

			if domainDB != nil {
				if tx, err := domainDB.BeginTransaction(ctx, sop.ForReading); err == nil {
					kb, err := domainDB.OpenKnowledgeBase(ctx, domain, tx, nil, nil)
					if err == nil && a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
						vecs, err := a.service.Domain().Embedder().EmbedTexts(ctx, []string{query})
						if err == nil && len(vecs) > 0 {
							closestCat, _, err := kb.Manager.FindClosestCategory(ctx, vecs[0])
							catFilter := ""
							if err == nil && closestCat != nil {
								catFilter = closestCat.Name
							}
							hits, err := kb.SearchSemantics(ctx, vecs[0], &memory.SearchOptions[map[string]any]{Limit: 5, Category: catFilter})
							hasGoodHits := false
							accumStr := ""
							if err == nil && len(hits) > 0 {
								for _, hit := range hits {
									if hit.Score < 0.6 {
										continue
									}
									hasGoodHits = true
									valStr := ""
									if str, ok := hit.Payload["_raw_content"].(string); ok {
										valStr = str
									} else if str, ok := hit.Payload["content"].(string); ok {
										valStr = str
									} else {
										valStr = fmt.Sprintf("%v", hit.Payload)
									}
									accumStr += fmt.Sprintf("- Context (Score: %.2f): %s\n", hit.Score, valStr)
								}
							}

							if hasGoodHits {
								toolsDef += fmt.Sprintf("\nActive Playbook Context (%s):\n%s", domain, accumStr)
								a.MarkMRUCategory(domain, fmt.Sprintf("Retrieved Semantics:\n%s", accumStr))
							} else {
								if a.service != nil && a.service.session != nil {
									a.service.session.MRUMu.RLock()
									for _, item := range a.service.session.MRU {
										if item.Category == domain && item.Context != "" {
											toolsDef += fmt.Sprintf("\nCarried-Over Playbook Context (%s):\n%s\n", domain, item.Context)
											break
										}
									}
									a.service.session.MRUMu.RUnlock()
								}
							}
						}
					}
					tx.Rollback(ctx)
				}
			}
		}
	}

	if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" && !isSpaceGeneration {
		var db *database.Database
		if p.CurrentDB == SystemDBName {
			db = a.systemDB
		} else if dbOpts, ok := a.databases[p.CurrentDB]; ok {
			db = database.NewDatabase(dbOpts)
		}

		if db != nil {
			if tx, err := db.BeginTransaction(ctx, sop.ForReading); err == nil {
				if stores, err := tx.GetStores(ctx); err == nil {
					toolsDef += fmt.Sprintf("\nActive Database: %s\nAvailable Stores:\n", p.CurrentDB)
					for _, s := range stores {
						if strings.Contains(s, "/") {
							continue
						}
						var schemaInfo string
						if storeAccessor, err := jsondb.OpenStore(ctx, db.Config(), s, tx); err == nil {
							info := storeAccessor.GetStoreInfo()
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

	toolsDef += a.getSystemInstructions(ctx, query)

	// Reconstruct the active transcript dynamically from episodic Short-Term Memory
	convHistory := ""
	if a.service != nil && a.service.session != nil && a.service.session.Memory != nil {
		if thread := a.service.session.Memory.GetCurrentThread(); thread != nil && len(thread.Exchanges) > 0 {
			var sb strings.Builder

			exchanges := thread.Exchanges
			if len(exchanges) > MaxExchangesInHistory {
				exchanges = exchanges[len(exchanges)-MaxExchangesInHistory:]
			}

			for _, ex := range exchanges {
				if ex.Role == RoleUser {
					sb.WriteString(fmt.Sprintf("User: %s\n", ex.Content))
				} else if ex.Role == RoleAssistant {
					sb.WriteString(fmt.Sprintf("Assistant: %s\n", ex.Content))
				}
			}
			if sb.Len() > 0 {
				convHistory = "\n[ACTIVE SESSION MEMORY]\n" + sb.String() + "\n[/ACTIVE SESSION MEMORY]\n\n"
			}
		}
	}

	return toolsDef + "\n" + convHistory + "User: " + query
}

// ============================================================================
// BASELINE ReACT ENGINE (10 Turn Loop Abstraction)
// ============================================================================

type BaselineReActEngine struct {
	Agent *CopilotAgent
}

func (b *BaselineReActEngine) Run(ctx context.Context, req ai.ReasoningRequest) (ai.ReasoningResponse, error) {
	maxTurns := 10
	history := req.SystemPrompt
	isSpaceGeneration := strings.Contains(req.UserQuery, "IMPORTANT SYSTEM RULE: The user is generating a UI Space.")

	for i := 0; i < maxTurns; i++ {
		// Use injected Generator (gen)
		resp, err := req.Generator.Generate(ctx, history, ai.GenOptions{})
		if err != nil {
			return ai.ReasoningResponse{}, err
		}

		text := strings.TrimSpace(resp.Text)
		isToolCall := false
		var cleanText string
		if start := strings.Index(text, "```"); start != -1 {
			cleanText = text[start:]
			if strings.HasPrefix(cleanText, "```json") {
				cleanText = strings.TrimPrefix(cleanText, "```json")
			} else {
				cleanText = strings.TrimPrefix(cleanText, "```")
			}
			if end := strings.LastIndex(cleanText, "```"); end != -1 {
				cleanText = cleanText[:end]
			}
			isToolCall = true
		} else {
			idxOb := strings.Index(text, "{")
			idxAr := strings.Index(text, "[")
			if idxOb != -1 && (idxAr == -1 || idxOb < idxAr) {
				cleanText = text[idxOb:]
				isToolCall = true
			} else if idxAr != -1 {
				cleanText = text[idxAr:]
				isToolCall = true
			}
		}

		if isToolCall && (strings.Contains(cleanText, "\"tool\"") || strings.Contains(cleanText, "\"op\"")) {
			cleanText = strings.TrimSpace(cleanText)
			type localToolCall struct {
				Tool string         `json:"tool"`
				Args map[string]any `json:"args"`
			}
			var toolCalls []localToolCall

			if err := json.Unmarshal([]byte(cleanText), &toolCalls); err == nil {
				validToolCalls := true
				if len(toolCalls) > 0 {
					for _, tc := range toolCalls {
						if tc.Tool == "" {
							validToolCalls = false
							break
						}
					}
				}
				if !validToolCalls || len(toolCalls) == 0 {
					toolCalls = nil
				}
			}

			if len(toolCalls) == 0 {
				var scriptSteps []any
				if err := json.Unmarshal([]byte(cleanText), &scriptSteps); err == nil && len(scriptSteps) > 0 {
					isScript := true
					for _, step := range scriptSteps {
						if m, ok := step.(map[string]any); ok {
							if _, hasOp := m["op"]; !hasOp {
								isScript = false
								break
							}
						} else {
							isScript = false
							break
						}
					}
					if isScript {
						log.Info("Auto-detected raw script in LLM output. Wrapping in 'execute_script' tool.")
						toolCalls = []localToolCall{
							{
								Tool: "execute_script",
								Args: map[string]any{
									"script": scriptSteps,
								},
							},
						}
					}
				}
			}

			if len(toolCalls) == 0 {
				var single localToolCall
				if err2 := json.Unmarshal([]byte(cleanText), &single); err2 == nil && single.Tool != "" {
					toolCalls = []localToolCall{single}
				}
			}

			if len(toolCalls) > 0 {
				isCtxVerbose, _ := ctx.Value("verbose").(bool)
				if b.Agent.Config.Verbose || isCtxVerbose {
					if w, ok := ctx.Value(ai.CtxKeyWriter).(io.Writer); ok {
						var prettyJSON bytes.Buffer
						if err := json.Indent(&prettyJSON, []byte(cleanText), "", "  "); err == nil {
							fmt.Fprintf(w, "\n[Tool Instructions]:\n%s\n", prettyJSON.String())
						} else {
							fmt.Fprintf(w, "\n[Tool Instructions]:\n%s\n", cleanText)
						}
					}
				}

				if p := ai.GetSessionPayload(ctx); p != nil {
					p.LastInteractionSteps = 0
				}

				var results []string
				for _, tc := range toolCalls {
					if isSpaceGeneration && (tc.Tool == "open_store" || tc.Tool == "execute_script" || tc.Tool == "list_stores") {
						return ai.ReasoningResponse{FinalText: "SYSTEM ERROR: DO NOT output any tool callbacks for Space generation. Please emit purely raw ExportData JSON object without 'tool' formatting."}, nil
					}
					result, err := req.Executor.Execute(ctx, tc.Tool, tc.Args)
					if err != nil {
						result = "Error: " + err.Error()
					}
					results = append(results, result)

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

				finalResp := strings.Join(results, "\n")
				tcBytes, _ := json.Marshal(toolCalls)
				b.Agent.logThought(ctx, req.UserQuery, string(tcBytes), finalResp)

				history += "\n\nAssistant:\n" + text + "\n\n[System Tool Response]:\n" + finalResp + "\n\nUser: Analyze the tool response and decide the next step. If the task is fully complete, provide your final response to the user with NO JSON or tool markup."
				continue
			}
		}

		return ai.ReasoningResponse{FinalText: text}, nil
	}

	return ai.ReasoningResponse{FinalText: "Error: Maximum turns reached."}, nil
}

// ListTools returns the list of available tools.
func (a *CopilotAgent) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	var tools []ai.ToolDefinition

	// Append compiled go tools
	if a.registry != nil {
		for _, t := range a.registry.List() {
			if t.Hidden {
				continue // Skip hidden for the LLM natively as well
			}
			tools = append(tools, ai.ToolDefinition{
				Name:        t.Name,
				Description: t.Description,
				Schema:      t.ArgsSchema,
			})
		}
	}

	// Query systemDB for stored user scripts
	if a.systemDB != nil {
		tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
		if err == nil {
			defer tx.Rollback(ctx)
			store, err := a.systemDB.OpenModelStore(ctx, "scripts", tx)
			if err == nil {
				// Iterate via Store.List
				// For simplicity, scripts might not have explicit schema strings yet,
				// but we build a minimal one if properties are available.
				var defaultArgsSchema = `{"type": "object", "properties": {"database": {"type": "string", "description": "Target database constraint (optional)"}}}`

				// Attempt to load all scripts under ai.DefaultScriptCategory
				var keys []string
				keys, _ = store.List(ctx, ai.DefaultScriptCategory)
				for _, scriptName := range keys {
					var script ai.Script
					if err := store.Load(ctx, ai.DefaultScriptCategory, scriptName, &script); err == nil {
						desc := script.Description
						if desc == "" {
							desc = "Executes the script '" + scriptName + "'"
						}

						tools = append(tools, ai.ToolDefinition{
							Name:        scriptName,
							Description: "Execute pre-saved user script. " + desc,
							Schema:      defaultArgsSchema,
						})
					}
				}
			}
		}
	}

	return tools, nil
}

// InitializePhysicalMemory creates strictly isolated B-Tree (STM) and Vector (LTM) structures for this Agent ID
func (a *CopilotAgent) InitializePhysicalMemory(ctx context.Context) error {
	if a.Memory == nil {
		return fmt.Errorf("MemoryUnit is nil")
	}
	if a.systemDB == nil {
		log.Warn("CopilotAgent: systemDB is nil, skipping physical STM/LTM initialization")
		return nil
	}
	if a.Memory.AgentID == "" {
		a.Memory.AgentID = "omni"
	}

	// Dynamic store naming based on Avatar ID to ensure physical strict isolation
	stmStoreName := fmt.Sprintf("stm_%s", a.Memory.AgentID)

	tx, err := a.systemDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for isolated STM: %w", err)
	}

	// In the real system, you might want a specialized Opener or B-Tree logic. B-Tree auto creates if not exist (using NewBtree if Open fails).
	store, err := a.systemDB.OpenBtree(ctx, stmStoreName, tx)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "not found") {
			// Because OpenBtree failed, SOP likely rolled back the transaction. Create a new one.
			tx, err = a.systemDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Errorf("failed to begin new transaction for isolated STM: %w", err)
			}
			if !tx.HasBegun() {
				if err := tx.Begin(ctx); err != nil {
					tx.Rollback(ctx)
					return fmt.Errorf("failed to start transaction: %w", err)
				}
			}
			store, err = a.systemDB.NewBtree(ctx, stmStoreName, tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("failed to create isolated STM BTree: %w", err)
			}
		} else {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to open isolated STM BTree: %w", err)
		}
	}

	ltmStoreName := fmt.Sprintf("ltm_%s", a.Memory.AgentID)
	ltm, err := a.systemDB.OpenKnowledgeBase(ctx, ltmStoreName, tx, a.brain, nil)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("failed to open isolated LTM KnowledgeBase: %w", err)
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit isolated physical memory initialization: %w", err)
	}

	a.Memory.STM = store
	a.Memory.LTM = ltm
	log.Debug("Initialized Isolated Physical Memory for Avatar", "agent_id", a.Memory.AgentID, "stm_store", stmStoreName, "ltm_store", ltmStoreName)

	// Wire up Cognitive Memory background workers for the Avatar
	// 1. Batch short-term flushing (Working Memory -> STM)
	a.StartMemoryWorkers(ctx)

	// 2. Schedule Sleep Cycle consolidator (STM -> LTM)
	hourlyInterval := a.Config.SleepCycleIntervalHours
	idleTimeout := a.Config.IdleSleepTimeoutMinutes

	a.StartSleepCycle(ctx, hourlyInterval, idleTimeout, nil)

	return nil
}

// logEpisodeToSTM directly writes to the Agent's physical STM structure
func (a *CopilotAgent) logEpisodeToSTM(ctx context.Context, intent string, astPayload any, outcome string, executeErr error) {
	if a.Memory == nil || a.Memory.STM == nil {
		return
	}

	astBytes, err := json.Marshal(astPayload)
	var astStr string
	if err == nil {
		astStr = string(astBytes)
	} else {
		astStr = fmt.Sprintf("%T", astPayload)
	}

	status := "Success"
	errorDesc := ""
	if executeErr != nil {
		status = "Error"
		errorDesc = executeErr.Error()
	}

	// Combine into a structured representation for embedding and retrieval
	thought := fmt.Sprintf("Intent: %s\nAST: %s\nStatus: %s\n", intent, astStr, status)
	if errorDesc != "" {
		thought += fmt.Sprintf("Error: %s\n", errorDesc)
	}
	if status == "Success" && outcome != "" {
		outLog := outcome
		if len(outLog) > 100 {
			outLog = outLog[:100] + "..."
		}
		thought += fmt.Sprintf("Outcome: %s\n", outLog)
	}

	hash := sha256.Sum256([]byte(thought))
	itemID := fmt.Sprintf("%x", hash)

	payload := map[string]any{
		"id":         itemID,
		"intent":     intent,
		"thought":    thought,
		"status":     status,
		"outcome":    outcome,
		"created_at": time.Now().UnixMilli(),
		"agent_id":   a.Memory.AgentID,
	}

	a.lastEpisodeTS.Store(time.Now().UnixMilli())

	select {
	case a.episodeQueue <- payload:
		log.Debug("Isolated STM: Buffered thought snippet to queue successfully", "agent_id", a.Memory.AgentID)
	default:
		log.Warn("Isolated STM: Episode queue is full, dropping thought snippet", "agent_id", a.Memory.AgentID)
	}
}

func (a *CopilotAgent) logThought(ctx context.Context, query string, toolsExecuted string, outcome string) {
	if a.systemDB == nil {
		return
	}

	payloadInfo := ai.GetSessionPayload(ctx)
	userID := "default"
	kbName := fmt.Sprintf("%s%s", ai.MemoryKBPrefix, userID)
	if payloadInfo != nil && payloadInfo.UserID != "" {
		userID = payloadInfo.UserID
		kbName = payloadInfo.GetMemoryKBName()
	}

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
		embedder = a.service.Domain().Embedder()
	}
	if embedder == nil {
		return
	}

	status := "Success"
	thought := fmt.Sprintf("Intent: %s\nAST: %s\nStatus: %s\nOutcome: %s\n", query, toolsExecuted, status, outcome)

	go func() {
		embedCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		vecs, err := embedder.EmbedTexts(embedCtx, []string{thought})
		if err != nil || len(vecs) == 0 {
			return
		}
		vec := vecs[0]

		tx, err := a.systemDB.BeginTransaction(embedCtx, sop.ForWriting)
		if err != nil {
			return
		}

		kb, err := a.systemDB.OpenKnowledgeBase(embedCtx, kbName, tx, a.brain, embedder)
		if err != nil {
			tx.Rollback(embedCtx)
			return
		}

		hash := sha256.Sum256([]byte(thought))
		itemID := fmt.Sprintf("%x", hash)

		payload := map[string]any{
			"id":      itemID,
			"intent":  query,
			"ast":     toolsExecuted,
			"status":  status,
			"outcome": outcome,
			"ts":      time.Now().UnixMilli(),
			"type":    "episode",
		}
		if err := kb.IngestThought(embedCtx, thought, "", "System", vec, payload); err != nil {
			tx.Rollback(embedCtx)
			return
		}
		tx.Commit(embedCtx)
		log.Debug("Active Memory: Episode logged successfully", "temp_id", itemID)
	}()
}

// Execute executes the requested tool against the session payload.
func (a *CopilotAgent) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
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
	// BUT: If the tool is "save_last_step", we should NOT overwrite the last tool call yet!
	// We need to let it run using the *previous* last tool call.
	// So we defer the update of lastToolCall until AFTER execution, OR we skip it for meta-tools.

	isMetaTool := toolName == "save_last_step"

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
		// However, for "save_last_step", the user might want to see the *previous* tool.
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
		if dbName == SystemDBName && a.systemDB != nil {
			dbFound = true
		} else if _, ok := a.databases[dbName]; ok {
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
		if a.systemDB != nil {
			keys = append(keys, SystemDBName)
		}
		for k := range a.databases {
			keys = append(keys, k)
		}
		return "", fmt.Errorf("database not found or not selected (Copilot). Requested: '%s', Available: %v", dbName, keys)
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
				if err := store.Load(ctx, ai.DefaultScriptCategory, toolName, &script); err == nil {
					// Found script! Execute it.
					return a.runScript(ctx, toolName, script, args)
				}
			}
		}
	}

	return "", fmt.Errorf("unknown tool: %s", toolName)
}

func (a *CopilotAgent) runScript(ctx context.Context, name string, script ai.Script, args map[string]any) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Running script '%s'...\n", name))

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
				sb.WriteString(fmt.Sprintf("%s\n\n", res))
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

func (a *CopilotAgent) runScriptRaw(ctx context.Context, script ai.Script, args map[string]any) (string, error) {
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

func (a *CopilotAgent) deobfuscateMap(m map[string]any) {
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

func (a *CopilotAgent) deobfuscateValue(v any) any {
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

func (a *CopilotAgent) classifyIntent(ctx context.Context, query string, gen ai.Generator) string {
	if flag.Lookup("test.v") != nil {
		// Bypass LLM-based intent routing in unit tests
		return "OMNI"
	}

	availableKBs := ""
	if p := ai.GetSessionPayload(ctx); p != nil && len(p.SelectedKBs) > 0 {
		availableKBs = strings.Join(p.SelectedKBs, ", ")
	}

	sysPrompt := `You are an ultra-fast Intent Classifier Router. Determine if the user is asking a general Omni Copilot question, or explicitly directing a query to a domain-specific Avatar or Persona.`
	if availableKBs != "" {
		sysPrompt += fmt.Sprintf("\n\nAvailable Avatars/KBs in the user's current workspace dropdown context: [%s].", availableKBs)
	}

	sysPrompt += `
Note: The "OMNI" Copilot is the primary expert on the SOP platform itself. Any questions regarding general SOP configuration, Data Stores, Spaces, SOP in SDLC, platform troubleshooting, or core platform tools should be classified as "OMNI".

If the request is general or pertains to the SOP platform, output EXACTLY the word "OMNI".
If they specifically name or target an Avatar/domain from the Available Avatars list, output ONLY the exact name of that Avatar. Do not output any other formatting or punctuation.`

	opts := ai.GenOptions{
		SystemPrompt: sysPrompt,
		Temperature:  0.1, // Fast, deterministic routing
	}

	out, err := gen.Generate(ctx, query, opts)
	if err != nil {
		log.Warn("Intent Classification failed, defaulting to OMNI", "error", err)
		return "OMNI"
	}

	res := strings.TrimSpace(out.Text)
	res = strings.Trim(res, "\"'*")

	if strings.ToUpper(res) == "OMNI" || res == "" {
		return "OMNI"
	}
	return res
}
