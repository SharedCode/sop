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
	"github.com/sharedcode/sop/jsondb"
)

const (
	MaxMRUSize            = 20
	MaxExchangesInHistory = 20
)

const (
	MRUSourceUnknown     = ""
	MRUSourcePersona     = "persona"
	MRUSourceSystemTools = "system_tools"
	MRUSourcePlaybook    = "playbook"
)

// MRUItem represents a single category currently in working memory
type MRUItem struct {
	Category     string
	LastAccessed int64
	Context      string
	Source       string
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
	Memory *memory.MemoryUnit

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
		Memory: &memory.MemoryUnit{
			AgentID: ai.AgentIDOmni, // By default Clone behaves as Omni, Avatars explicitly override
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
	a.markMRUCategoryWithSource(category, context, MRUSourceUnknown)
}

func (a *CopilotAgent) markMRUCategoryWithSource(category string, context string, source string) {
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
			if source != "" {
				sess.MRU[i].Source = source
			}
			return
		}
	}

	// Add new
	sess.MRU = append(sess.MRU, MRUItem{
		Category:     category,
		LastAccessed: ts,
		Context:      context,
		Source:       source,
	})
	// Sort by newest and shrink if > MaxMRUSize
	if len(sess.MRU) > MaxMRUSize {
		sort.Slice(sess.MRU, func(i, j int) bool {
			return sess.MRU[i].LastAccessed > sess.MRU[j].LastAccessed
		})
		sess.MRU = sess.MRU[:MaxMRUSize]
	}
}

func (a *CopilotAgent) clearMRUCategory(category string) {
	if a.service == nil || a.service.session == nil {
		return
	}
	sess := a.service.session
	sess.MRUMu.Lock()
	defer sess.MRUMu.Unlock()

	filtered := sess.MRU[:0]
	for _, item := range sess.MRU {
		if item.Category != category {
			filtered = append(filtered, item)
		}
	}
	sess.MRU = filtered
}

func (a *CopilotAgent) clearMRUForTopicSwitch() {
	if a.service == nil || a.service.session == nil {
		return
	}
	sess := a.service.session
	sess.MRUMu.Lock()
	defer sess.MRUMu.Unlock()

	filtered := sess.MRU[:0]
	for _, item := range sess.MRU {
		if item.Source == MRUSourcePersona || strings.HasPrefix(item.Category, "PERSONA_") {
			filtered = append(filtered, item)
		}
	}
	sess.MRU = filtered
}

func playbookMRUCategory(domain string) string {
	domain = strings.TrimSpace(domain)
	if domain == "" {
		return ""
	}
	return "PLAYBOOK_" + domain
}

// GetMRUCategory retrieves a category from the global working memory MRU
func (a *CopilotAgent) GetMRUCategory(category string) (string, bool) {
	return a.getMRUCategoryBySource(category, MRUSourceUnknown, false)
}

func (a *CopilotAgent) getMRUCategoryBySource(category string, source string, allowLegacyUnknown bool) (string, bool) {
	if item, ok := a.findMRUItem(category, source, allowLegacyUnknown); ok {
		return item.Context, true
	}
	return "", false
}

func (a *CopilotAgent) findMRUItem(category string, source string, allowLegacyUnknown bool) (MRUItem, bool) {
	if a.service == nil || a.service.session == nil {
		return MRUItem{}, false
	}
	sess := a.service.session
	sess.MRUMu.RLock()
	defer sess.MRUMu.RUnlock()

	var legacyFallback MRUItem
	var hasLegacyFallback bool
	for _, item := range sess.MRU {
		if item.Category == category {
			if source == MRUSourceUnknown || item.Source == source {
				return item, true
			}
			if allowLegacyUnknown && item.Source == MRUSourceUnknown && !hasLegacyFallback {
				legacyFallback = item
				hasLegacyFallback = true
			}
		}
	}
	if hasLegacyFallback {
		return legacyFallback, true
	}
	return MRUItem{}, false
}

func (a *CopilotAgent) getMRUSnapshot() []MRUItem {
	if a.service == nil || a.service.session == nil {
		return nil
	}
	sess := a.service.session
	sess.MRUMu.RLock()
	defer sess.MRUMu.RUnlock()
	return append([]MRUItem(nil), sess.MRU...)
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
		Memory:    memory.NewMemoryUnit(ai.AgentIDOmni),
		// API Keys are less relevant now that we use the Generator interface mostly,
		// but we keep them empty or fill them if we really need them for direct calls later.
		// geminiKey:       geminiKey,
		// openAIKey:       openAIKey,
		compiledScripts: make(map[string]CachedScript),
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
	if p != nil {
		p.Close(ctx)
	}
	return nil
}

// Search performs a search using the agent's capabilities.
func (a *CopilotAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return []ai.Hit[map[string]any]{}, nil
}

// Ask is the primary entry point for processing a user's conversational query to the AI Copilot.
//
// The functional flow follows a multi-layered ReAct architecture:
//
//  1. Generator Resolution:
//     Dynamically selects the appropriate LLM provider (e.g., Gemini, OpenAI, Claude) based on
//     the active session context or falling back to the system default configuration.
//
//  2. Direct Invocation Handling:
//     Checks if the query is a direct tool invocation (e.g., "/help" or "/clear_memory").
//     If so, it bypasses the LLM reasoning loop to immediately execute the deterministic command.
//
//  3. Intent Classification (Router):
//     Evaluates if the query should be routed to a specific sub-agent (Avatar/Persona).
//     If the intent indicates a specialized Avatar (e.g., a "Legal Auditor" persona), it delegates
//     execution entirely to that sub-agent.
//
//  4. Context Classification (Domain Injection):
//     For generic queries (intent == "OMNI"), it performs a lightweight classification to identify
//     the semantic domain (e.g., "Spaces" or "Stores"). Based on this domain, it forcefully injects
//     relevant tool documentations (from the system KB) into the semantic memory buffer.
//
//  5. Episode Metadata Tracking (MRU Cache):
//     Analyzes the user's prior chat exchange inside the short-term episodic memory. If the user
//     remains engaged in the same topic and database context, it pulls the Most-Recently-Used (MRU)
//     semantic boundaries so the LLM retains coherent situational context across turns.
//
//  6. System Prompt Construction:
//     Assembles the massive multi-part context prompt (using SystemPromptBuilder) linking the
//     Core Persona, Active Playbooks/KBs, injected Tool Descriptions, semantic memory boundaries,
//     and chronological conversation history.
//
//  7. Reasoning Engine Delegation:
//     Packages the assembled context and delegates execution to the ReAct engine. The engine loops
//     autonomously over the LLM generation and local tool executions (API-level tool calling) until
//     it produces a final answer.
//
//  8. Epilogue & Cleanup:
//     Records the completed dialogue and active track-state into the short-term memory transcript,
//     clears the volatile MRU buffer, and returns the final text response to the client.
func (a *CopilotAgent) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	if len(opts) > 0 {
		cfg := ai.NewAskConfig(opts...)
		if format, ok := cfg.Values["default_format"].(string); ok && strings.TrimSpace(format) != "" {
			ctx = context.WithValue(ctx, ai.CtxKeyDefaultFormat, strings.TrimSpace(format))
		}
	}

	// 1. Generator Resolution
	gen := a.resolveGenerator(ctx)
	if gen == nil {
		return "⚠️ **AI Copilot Disabled**: No valid API Key found.\n\nPlease go to **Environment Settings** (HDD icon in bottom left) -> **LLM API Key** to configure your Google Gemini or OpenAI key.", nil
	}

	var sessionID string
	var currentDB string
	var activeDomain string
	if p := ai.GetSessionPayload(ctx); p != nil {
		sessionID = p.SessionID
		currentDB = p.CurrentDB
		activeDomain = p.ActiveDomain
	}
	currentThreadID := ""
	if a.service != nil && a.service.session != nil && a.service.session.Memory != nil {
		if thread := a.service.session.Memory.GetCurrentThread(); thread != nil {
			currentThreadID = fmt.Sprintf("%v", thread.ID)
		}
	}
	log.Info("Copilot Ask Start",
		"generator", gen.Name(),
		"default_format", getRequestedOutputFormat(ctx),
		"session_id", sessionID,
		"current_db", currentDB,
		"active_domain", activeDomain,
		"thread_id", currentThreadID,
		"query_chars", len(query),
	)

	if handled, res, err := a.handlePendingUserConfirmation(ctx, query); handled {
		return res, err
	}

	// 2. Direct Invocation Handling
	if handled, res, err := a.handleDirectInvocation(ctx, query, gen); handled {
		return res, err
	}

	// 3. Intent Classification (Router)
	intent := a.classifyIntent(ctx, query, gen)

	// Fast-path routing: If Avatar, execute Avatar Sub-Agent
	if intent != ai.IntentOmni {
		log.Info("Ask: Request classified for Avatar", "avatar", intent)
		return a.executeAvatarSubAgent(ctx, intent, query)
	}
	log.Info("Ask: Request classified for OMNI")

	// 4. Three-Gate Context Classification (Domain & Tool Injection)
	taskContext := a.evaluateRoutingGates(ctx, query, gen)
	if taskContext == nil {
		taskContext = &TaskContextClassification{Domain: "General"}
	}
	log.Info("Copilot Ask Routing",
		"routing_gate", taskContext.RoutingGate,
		"task_context", summarizeTaskContextForLog(*taskContext),
	)

	// 5. Episode Metadata Tracking (MRU Cache)
	a.trackEpisodeMetadata(ctx, intent)

	a.registerTools(ctx)

	// 6. System Prompt Construction
	fullPrompt := a.buildSystemPrompt(ctx, query, *taskContext)

	// 7. Reasoning Engine Delegation
	finalText, err := a.delegateToReasoningEngine(ctx, query, gen, fullPrompt)
	if err != nil {
		return "", err
	}
	log.Info("Copilot Ask Complete",
		"session_id", sessionID,
		"response_chars", len(finalText),
	)

	// 8. Epilogue & Cleanup
	a.epilogueAndCleanup(ctx, query, intent, finalText)

	return finalText, nil
}

func (a *CopilotAgent) handleDirectInvocation(ctx context.Context, query string, gen ai.Generator) (bool, string, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "/") {
		// Register tools for local command parsing
		a.registerTools(ctx)
		handled, res, err := a.handleSlashCommand(ctx, query, gen)
		if handled {
			if err != nil {
				return true, res, err
			}
			return true, res, nil
		}
	}
	return false, "", nil
}

func (a *CopilotAgent) handlePendingUserConfirmation(ctx context.Context, query string) (bool, string, error) {
	if a.service == nil || a.service.session == nil {
		return false, "", nil
	}

	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return false, "", nil
	}

	session := a.service.session
	session.mu.Lock()

	if pending := session.PendingConfirmation; pending != nil {
		if isAffirmativeConfirmation(trimmed) {
			session.PendingConfirmation = nil
			session.mu.Unlock()
			args := map[string]any{"kb_name": pending.SpaceName}
			if pending.DatabaseName != "" {
				args["database"] = pending.DatabaseName
			}
			res, err := a.toolDeleteSpace(ctx, args)
			if err == nil {
				res = "[[CLEAR_PENDING_CONFIRMATION]]\n" + res
			}
			return true, res, err
		}
		if isNegativeConfirmation(trimmed) {
			session.PendingConfirmation = nil
			session.mu.Unlock()
			return true, fmt.Sprintf("[[CLEAR_PENDING_CONFIRMATION]]\nCancelled deletion of Space '%s'.", pending.SpaceName), nil
		}
		session.mu.Unlock()
		return true, fmt.Sprintf("Pending deletion confirmation for Space '%s'. Reply 'yes' to confirm or 'no' to cancel.", pending.SpaceName), nil
	}

	spaceName, ok := parseDeleteSpaceRequest(trimmed)
	if !ok {
		session.mu.Unlock()
		return false, "", nil
	}

	dbName := ""
	if p := ai.GetSessionPayload(ctx); p != nil {
		dbName = p.CurrentDB
	}
	session.PendingConfirmation = &PendingUserConfirmation{
		Kind:         "delete_space",
		SpaceName:    spaceName,
		DatabaseName: dbName,
	}
	session.mu.Unlock()
	if dbName != "" {
		return true, fmt.Sprintf("Delete Space '%s' from database '%s'? Reply 'yes' to confirm or 'no' to cancel.", spaceName, dbName), nil
	}
	return true, fmt.Sprintf("Delete Space '%s'? Reply 'yes' to confirm or 'no' to cancel.", spaceName), nil
}

func parseDeleteSpaceRequest(query string) (string, bool) {
	trimmed := strings.TrimSpace(strings.TrimRight(lastNonEmptyQueryLine(query), ".!?"))
	if trimmed == "" {
		return "", false
	}
	lower := strings.ToLower(trimmed)
	patterns := []string{
		"delete space ",
		"delete the space ",
		"remove space ",
		"remove the space ",
		"delete knowledge base ",
		"delete the knowledge base ",
		"remove knowledge base ",
		"remove the knowledge base ",
		"delete kb ",
		"remove kb ",
	}
	for _, pattern := range patterns {
		idx := strings.Index(lower, pattern)
		if idx >= 0 {
			name := strings.TrimSpace(trimmed[idx+len(pattern):])
			name = strings.TrimPrefix(name, ":")
			name = strings.TrimSpace(name)
			name = strings.Trim(name, "\"'`")
			if cut := strings.IndexAny(name, ".!?"); cut >= 0 {
				name = strings.TrimSpace(name[:cut])
			}
			if name != "" {
				return name, true
			}
		}
	}

	reversePrefixes := []string{"delete ", "remove "}
	reverseSuffixes := []string{" space", " kb", " knowledge base"}
	for _, prefix := range reversePrefixes {
		if !strings.HasPrefix(lower, prefix) {
			continue
		}
		remainder := strings.TrimSpace(trimmed[len(prefix):])
		lowerRemainder := strings.ToLower(remainder)
		for _, suffix := range reverseSuffixes {
			if !strings.HasSuffix(lowerRemainder, suffix) {
				continue
			}
			name := strings.TrimSpace(remainder[:len(remainder)-len(suffix)])
			name = strings.Trim(name, "\"'`")
			if cut := strings.IndexAny(name, ".!?"); cut >= 0 {
				name = strings.TrimSpace(name[:cut])
			}
			if name != "" {
				return name, true
			}
		}
	}
	return "", false
}

func isAffirmativeConfirmation(query string) bool {
	switch confirmationReplyToken(query) {
	case "yes", "y", "confirm", "confirmed", "delete", "delete it", "do it", "proceed":
		return true
	default:
		return false
	}
}

func isNegativeConfirmation(query string) bool {
	switch confirmationReplyToken(query) {
	case "no", "n", "cancel", "abort", "stop", "don't", "do not":
		return true
	default:
		return false
	}
}

func confirmationReplyToken(query string) string {
	trimmed := strings.TrimSpace(lastNonEmptyQueryLine(query))
	if trimmed == "" {
		return ""
	}

	return strings.ToLower(trimmed)
}

func lastNonEmptyQueryLine(query string) string {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return ""
	}

	lines := strings.Split(trimmed, "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return line
		}
	}

	return ""
}

func (a *CopilotAgent) evaluateRoutingGates(ctx context.Context, query string, gen ai.Generator) *TaskContextClassification {
	isTest := false
	if gen != nil {
		if strings.Contains(fmt.Sprintf("%T", gen), "mock") || strings.Contains(fmt.Sprintf("%T", gen), "Mock") || strings.Contains(fmt.Sprintf("%T", gen), "Smart") {
			isTest = true
		}
	}

	parts := strings.Split(query, ":")

	// Gate 1: Telescoping Prefix Verification
	if len(parts) > 1 && (strings.EqualFold(parts[0], "omni") || strings.EqualFold(parts[0], "medical") || strings.EqualFold(parts[0], "support")) {
		log.Info("Gate 1 Activated: Prefix match", "prefix", parts[0])

		var taskCtx *TaskContextClassification
		parsedEntity := parts[0]
		parsedDomain := ""
		parsedArtifact := ""
		if len(parts) >= 2 {
			parsedDomain = strings.TrimSpace(parts[1])
		}
		if len(parts) >= 3 {
			parsedArtifact = strings.TrimSpace(parts[2])
		}

		if !isTest && gen != nil {
			taskCtx, _ = a.ClassifyFocusedTaskContext(ctx, query, parsedEntity, parsedDomain, parsedArtifact, gen)
		}

		taskCtx = enrichFocusedTaskContext(taskCtx, parsedEntity, parsedDomain, parsedArtifact)
		annotateTaskContextIntent(taskCtx, query)
		taskCtx.RoutingGate = RoutingGateFocused

		a.injectToolsForDomain(ctx, taskCtx)
		if p := ai.GetSessionPayload(ctx); p != nil {
			if p.Variables == nil {
				p.Variables = make(map[string]any)
			}
			p.Variables["RoutingState"] = taskCtx
		}
		return taskCtx
	}

	// Gate 2: MRU Context Inheritance (Context Momentum)
	if p := ai.GetSessionPayload(ctx); p != nil && p.Variables != nil {
		if rs, ok := p.Variables["RoutingState"].(*TaskContextClassification); ok && rs != nil {
			if !isTest && gen != nil {
				updatedRS, isSwitch, err := a.ClassifyContinuityTaskContext(ctx, query, rs, gen)
				if err == nil && isSwitch {
					log.Info("Gate 2 Detected Topic Switch. Falling through to Gate 3.")
					delete(p.Variables, "RoutingState")
					a.clearMRUForTopicSwitch()

					// Clear the episodic memory thread to avoid context poisoning
					if a.service != nil && a.service.session != nil && a.service.session.Memory != nil {
						newThreadID := sop.NewUUID()
						thread := &ConversationThread{
							ID:         newThreadID,
							RootPrompt: query,
							Label:      "Topic Switch",
							Category:   "General",
							Exchanges:  make([]Interaction, 0),
							Status:     "active",
						}
						a.service.session.Memory.AddThread(thread)
						a.service.session.Memory.CurrentThreadID = newThreadID
					}

					// Fall through to Gate 3
				} else if err == nil && updatedRS != nil {
					log.Info("Gate 2 Activated: Inheriting MRU Context with Updates", "domain", updatedRS.Domain)
					annotateTaskContextIntent(updatedRS, query)
					updatedRS.RoutingGate = RoutingGateContinuity
					a.injectToolsForDomain(ctx, updatedRS)
					p.Variables["RoutingState"] = updatedRS
					return updatedRS
				}
			} else {
				// Test fallback
				log.Info("Gate 2 Activated: Inheriting MRU Context (Test Mode)", "domain", rs.Domain)
				annotateTaskContextIntent(rs, query)
				rs.RoutingGate = RoutingGateContinuity
				a.injectToolsForDomain(ctx, rs)
				return rs
			}
		}
	}

	// Gate 3: Cold Start & Context Outline Classification
	if len(parts) == 1 {
		log.Info("Gate 3 Activated: Cold Start Classification")
	}

	if gen != nil && !isTest {
		if taskCtx, err := a.ClassifyTaskContext(ctx, query, gen); err == nil && taskCtx != nil {
			log.Info("Gate 3 Classification Success", "domain", taskCtx.Domain)
			annotateTaskContextIntent(taskCtx, query)
			taskCtx.RoutingGate = RoutingGateDiscovery
			a.injectToolsForDomain(ctx, taskCtx)
			if p := ai.GetSessionPayload(ctx); p != nil {
				if p.Variables == nil {
					p.Variables = make(map[string]any)
				}
				p.Variables["RoutingState"] = taskCtx
			}
			return taskCtx
		} else {
			log.Warn("Gate 3 classification failed or returned nil", "error", err)
		}
	}

	return nil
}

func (a *CopilotAgent) injectToolsForDomain(ctx context.Context, taskCtx *TaskContextClassification) {
	if taskCtx == nil {
		return
	}

	if focused := a.buildFocusedToolContext(taskCtx); focused != "" {
		a.markMRUCategoryWithSource(SYSTEM_TOOLS, "\n"+focused, MRUSourceSystemTools)
		return
	}

	if strings.EqualFold(taskCtx.Domain, "Stores") {
		a.markMRUCategoryWithSource(SYSTEM_TOOLS, "\nStructured Context: Stores Tools\n"+toolsStoresManual, MRUSourceSystemTools)
	} else if strings.EqualFold(taskCtx.Domain, "Spaces") {
		a.markMRUCategoryWithSource(SYSTEM_TOOLS, "\nStructured Context: Spaces Tools\n"+toolsSpacesManual, MRUSourceSystemTools)
	}
}

func (a *CopilotAgent) trackEpisodeMetadata(ctx context.Context, intent string) {
	// Determine current target KB
	currentKBTrack := "sop"
	if p := ai.GetSessionPayload(ctx); p != nil && len(p.SelectedKBs) > 0 {
		var names []string
		for _, kb := range p.SelectedKBs {
			names = append(names, kb.Name)
		}
		currentKBTrack = "sop," + strings.Join(names, ",")
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
							a.markMRUCategoryWithSource(playbookMRUCategory(kb), "", MRUSourcePlaybook)
						}
					}
				}
			}
		}
	}
}

func (a *CopilotAgent) delegateToReasoningEngine(ctx context.Context, query string, gen ai.Generator, fullPrompt string) (string, error) {
	currentDB := ""
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDB = p.CurrentDB
	}

	// Obfuscate Prompt if enabled
	if a.shouldObfuscate(currentDB) {
		fullPrompt = obfuscation.GlobalObfuscator.ObfuscateText(fullPrompt)
	}

	// Active Implementation: Native Tools (API-level tool calling)
	engine := &NativeReActEngine{
		EnableObfuscation: a.shouldObfuscate(currentDB),
	}

	req := ai.ReasoningRequest{
		SystemPrompt: fullPrompt, // For baseline, this contains the full aggregated state
		UserQuery:    query,
		Executor:     a, // CopilotAgent implements Executor
		Generator:    gen,
	}

	resp, err := engine.Run(ctx, req)
	if err != nil {
		return "", err
	}

	finalText := resp.FinalText
	if a.shouldObfuscate(currentDB) {
		finalText = obfuscation.GlobalObfuscator.DeobfuscateText(finalText)
	}

	return finalText, nil
}

func (a *CopilotAgent) epilogueAndCleanup(ctx context.Context, query string, intent string, finalText string) {
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
			var names []string
			for _, kb := range p.SelectedKBs {
				names = append(names, kb.Name)
			}
			kbTrack = strings.Join(names, ",")
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
		mruSnapshot := a.getMRUSnapshot()

		thoughtPayload := map[string]any{
			"query":          query,
			"response":       finalText,
			"active_context": mruSnapshot,
		}

		go a.Memory.LogEpisodeToSTM(context.Background(), "user_interaction", thoughtPayload, "Interacted with user", nil)
	}
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

func (a *CopilotAgent) resolvePersona(ctx context.Context) string {
	agentID := ai.AgentIDOmni
	if a.Memory != nil && a.Memory.AgentID != "" {
		agentID = a.Memory.AgentID
	}
	cacheKey := "PERSONA_" + agentID

	// 1. Try MRU Cache
	if cachedVal, ok := a.getMRUCategoryBySource(cacheKey, MRUSourcePersona, true); ok && cachedVal != "" {
		return cachedVal
	}

	p := ai.GetSessionPayload(ctx)

	persona := ""
	if agentID != ai.AgentIDOmni {
		if p == nil {
			log.Error("Routed to an Avatar but there is no selected KB")
		} else {
			// If Avatar, use its exact KB as the Persona config source
			var matchingRef *ai.ArtifactReference
			for _, ref := range p.SelectedKBs {
				if ref.Name == agentID && ref.Type == ai.ArtifactTypeSpace {
					matchingRef = &ref
					break
				}
			}

			if matchingRef != nil {
				dbName := matchingRef.DatabaseName
				if dbName == "" {
					dbName = p.CurrentDB
				}

				var dbOpts sop.DatabaseOptions
				var found bool
				if dbOpts, found = a.databases[dbName]; found {
					// Use found opts
				} else if dbName == SystemDBName && a.systemDB != nil {
					dbOpts = a.systemDB.Config()
					found = true
				}

				if found && dbOpts.Type >= 0 {
					tempDB := database.NewDatabase(dbOpts)
					if tx, err := tempDB.BeginTransaction(ctx, sop.ForReading); err == nil {
						if kb, err := tempDB.OpenKnowledgeBase(ctx, matchingRef.Name, tx, nil, nil, false, true); err == nil {
							if cfg, err := kb.GetConfig(ctx); err == nil && cfg != nil && cfg.SystemPrompt != "" {
								persona = cfg.SystemPrompt + "\n\n"
							}
						}
						tx.Rollback(ctx)
					}
				}
			}
		}
	} else {
		// If Omni, strictly use the SOP KB as the Persona config source
		if a.systemDB != nil {
			tempDB := database.NewDatabase(a.systemDB.Config())
			if tx, err := tempDB.BeginTransaction(ctx, sop.ForReading); err == nil {
				if kb, err := tempDB.OpenKnowledgeBase(ctx, ai.DefaultKBName, tx, nil, nil, false, false); err == nil {
					if cfg, err := kb.GetConfig(ctx); err == nil && cfg != nil && cfg.SystemPrompt != "" {
						persona = cfg.SystemPrompt + "\n\n"
					}
				}
				tx.Rollback(ctx)
			}
		}
	}

	if persona == "" {
		persona = "You are a general-purpose intelligent AI Copilot equipped with a human-like 'active memory' system. " +
			"Your primary expertise is in a platform named SOP (which stands for 'Scalable Objects Persistence'). " +
			"SOP is an advanced data and AI platform that provides robust tooling for Knowledge Bases (KBs). " +
			"You aid users in SOP library adoption, technology integration, Spaces management, and data management. " +
			"As an expert in Scalable Objects Persistence, your core knowledge covers Databases, B-Trees, strict ACID Transactions, Swarm Computing, and advanced Storage mechanisms including Erasure Coding. " +
			"You understand that in this platform, a 'Space' or 'Knowledge Base' is a new AI memory subsystem combining VectorDB, Text Search, and a specialized schema (Thoughts: Category/Items), and you manage it differently than raw technical tables. " +
			"You have deep expertise in SOP scripting (AST-based execution), and the SOP HTTP API, covering request/response lifecycles, NDJSON streaming, and session management. " +
			"You derive your foundational knowledge, codebase context, and architectural principles directly from the source repository at https://github.com/sharedcode/sop. " +
			"Assist users dynamically with ANY open-ended request—whether answering general questions, creating and consulting Knowledge Bases, writing code, or managing database queries using the tools provided.\n\n"
	}

	persona += "CRITICAL SYSTEM GUARDRAIL:\n" +
		"1. Autonomous Research: You are an autonomous intelligent entity. You have implicit permission and are EXPECTED to use your 'Read' tools (e.g. Search KB, List/Query DB) to actively research Domains, Spaces (SOP or custom KBs), and codebase schemas as knowledge references. DO NOT proceed blindly if you lack context.\n" +
		"2. Disambiguation: If a user's request is ambiguous or lacks constraints, DO NOT guess or hallucinate parameters. Use your search tools to find relevant constraints first. If self-research fails, halt execution and explicitly consult the user for clarification.\n\n"

	// 2. Cache in MRU for future turns
	a.markMRUCategoryWithSource(cacheKey, persona, MRUSourcePersona)

	return persona
}

func (a *CopilotAgent) getScriptToolsPrompt(ctx context.Context) string {
	toolsDef := ""
	if a.systemDB != nil {
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
	return toolsDef
}

func (a *CopilotAgent) getLTMSemanticContext(ctx context.Context, query string) string {
	toolsDef := ""
	if a.systemDB != nil && a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil && a.Memory.AgentID != "" {
		if tx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading); err == nil {
			kbName := fmt.Sprintf("ltm_%s", a.Memory.AgentID)
			kb, err := a.systemDB.OpenKnowledgeBase(ctx, kbName, tx, nil, nil, false, true)
			if err == nil {
				vecs, err := a.service.Domain().Embedder().EmbedTexts(ctx, []string{query})
				if err == nil && len(vecs) > 0 {
					closestCat, _, err := kb.Manager.FindClosestCategory(ctx, vecs[0])
					catFilter := ""
					if err == nil && closestCat != nil {
						catFilter = closestCat.Name
					}
					hits, err := kb.SearchSemantics(ctx, vecs[0], &memory.SearchOptions[map[string]any]{Limit: 5, CategoryPath: catFilter})
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
	return toolsDef
}

func (a *CopilotAgent) getPlaybooksContext(ctx context.Context, query string, targetDomains []string) string {
	toolsDef := ""
	for _, domain := range targetDomains {
		domain = strings.TrimSpace(domain)
		if domain == "" || domain == "custom" {
			continue
		}

		var domainDB *database.Database

		var dbOptsList []sop.DatabaseOptions
		if a.systemDB != nil {
			dbOptsList = append(dbOptsList, a.systemDB.Config())
		}

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

		hasGoodHits := false
		if domainDB != nil {
			if tx, err := domainDB.BeginTransaction(ctx, sop.ForReading); err == nil {
				kb, err := domainDB.OpenKnowledgeBase(ctx, domain, tx, nil, nil, false)
				if err == nil && a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
					vecs, err := a.service.Domain().Embedder().EmbedTexts(ctx, []string{query})
					if err == nil && len(vecs) > 0 {
						closestCat, _, err := kb.Manager.FindClosestCategory(ctx, vecs[0])
						catFilter := ""
						if err == nil && closestCat != nil {
							catFilter = closestCat.Name
						}
						hits, err := kb.SearchSemantics(ctx, vecs[0], &memory.SearchOptions[map[string]any]{Limit: 5, CategoryPath: catFilter})
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
							a.markMRUCategoryWithSource(playbookMRUCategory(domain), fmt.Sprintf("Retrieved Semantics:\n%s", accumStr), MRUSourcePlaybook)
						}
					}
				}
				tx.Rollback(ctx)
			}
		}

		if !hasGoodHits {
			if a.service != nil && a.service.session != nil {
				if carriedOver, ok := a.getMRUCategoryBySource(playbookMRUCategory(domain), MRUSourcePlaybook, true); ok && carriedOver != "" {
					toolsDef += fmt.Sprintf("\nCarried-Over Playbook Context (%s):\n%s\n", domain, carriedOver)
				}
			}
		}
	}
	return toolsDef
}

func (a *CopilotAgent) getSchemaInjectionContext(ctx context.Context) string {
	toolsDef := ""
	if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
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
	return toolsDef
}

// buildSystemPrompt, on top of generated System Prompt(below), we rely on native Tool Calling via ListTools,
// which specifies each tool's basic info & supported JSON schema.
func (a *CopilotAgent) buildSystemPrompt(ctx context.Context, query string, taskClassification TaskContextClassification) string {
	builder := NewSystemPromptBuilder()

	// 1. Resolve Avatar / Custom KB Persona or Fallback
	builder.With(ComponentPersona, a.resolvePersona(ctx))

	// 2. LTM Semantic Resolution (Self-Correction / Working Memory)
	builder.With(ComponentSemanticMemory, a.getLTMSemanticContext(ctx, query))

	// 3. Always inject System Tools loaded into LTM
	systemTools := a.getSystemToolsContext(ctx)
	if focusedTools := a.buildFocusedToolContext(&taskClassification); focusedTools != "" && !strings.Contains(systemTools, focusedTools) {
		if systemTools != "" {
			systemTools += "\n\n"
		}
		systemTools += focusedTools
	}
	builder.With(ComponentSystemTools, systemTools)

	// 4. Active Custom KBs / Playbooks Lookups
	domains := []string{"sop"}
	if p := ai.GetSessionPayload(ctx); p != nil && p.ActiveDomain != "" {
		domains = append(domains, strings.Split(p.ActiveDomain, ",")...)
	}
	builder.With(ComponentPlaybooks, a.getPlaybooksContext(ctx, query, domains))
	builder.With(ComponentFocusedContext, a.getFocusedExecutionContext(ctx, taskClassification))

	// 5. Generic schema fallback only when no specific store targets were classified.
	if taskClassification.Domain == StoresDomain && len(taskClassification.DBArtifacts) == 0 {
		builder.With(ComponentSchema, a.getSchemaInjectionContext(ctx))
	}

	// 6. Reconstruct the active transcript dynamically from episodic Short-Term Memory
	builder.With(ComponentHistory, a.getSessionMemoryContext())

	// 7. Inject Final Trigger query
	builder.With(ComponentUserQuery, "User: "+query)

	// Render as highly structured JSON elements to prevent Prompt confusion
	fullPrompt, budgetReport := builder.ToJSONWithBudgetReport(a.promptBudgetProfile(taskClassification))
	log.Info("LLM Context Budget",
		"routing_gate", taskClassification.RoutingGate,
		"original_chars", budgetReport.OriginalTotalChars,
		"final_chars", budgetReport.FinalTotalChars,
		"trimmed_components", summarizePromptBudgetTrim(budgetReport),
	)
	log.Info("LLM Context (OMNI)", "SystemPrompt", fullPrompt)

	return fullPrompt
}

func (a *CopilotAgent) promptBudgetProfile(taskClassification TaskContextClassification) PromptBudgetProfile {
	profile := PromptBudgetProfile{
		TotalChars: 14000,
		ComponentCharBudgets: map[PromptComponent]int{
			ComponentPersona:        2800,
			ComponentSemanticMemory: 1400,
			ComponentSystemTools:    2600,
			ComponentPlaybooks:      1600,
			ComponentFocusedContext: 2600,
			ComponentSchema:         1800,
			ComponentHistory:        1800,
			ComponentUserQuery:      1200,
		},
		TrimPriorityLowToHigh: []PromptComponent{
			ComponentHistory,
			ComponentSchema,
			ComponentPlaybooks,
			ComponentSemanticMemory,
			ComponentSystemTools,
			ComponentPersona,
			ComponentFocusedContext,
			ComponentUserQuery,
		},
	}

	switch taskClassification.RoutingGate {
	case RoutingGateFocused:
		profile.TotalChars = 12500
		profile.ComponentCharBudgets[ComponentSemanticMemory] = 1000
		profile.ComponentCharBudgets[ComponentSystemTools] = 3200
		profile.ComponentCharBudgets[ComponentPlaybooks] = 1200
		profile.ComponentCharBudgets[ComponentFocusedContext] = 3400
		profile.ComponentCharBudgets[ComponentHistory] = 1200
	case RoutingGateContinuity:
		profile.TotalChars = 15000
		profile.ComponentCharBudgets[ComponentSemanticMemory] = 1800
		profile.ComponentCharBudgets[ComponentSystemTools] = 2400
		profile.ComponentCharBudgets[ComponentPlaybooks] = 1800
		profile.ComponentCharBudgets[ComponentHistory] = 2600
	}

	if isCrossDomain(taskClassification.Layers) {
		profile.TotalChars = 16500
		profile.ComponentCharBudgets[ComponentSystemTools] = 3600
		profile.ComponentCharBudgets[ComponentFocusedContext] = 3200
		profile.ComponentCharBudgets[ComponentPlaybooks] = 1800
	}

	if taskClassification.Domain == StoresDomain && len(taskClassification.DBArtifacts) == 0 {
		profile.ComponentCharBudgets[ComponentSchema] = 2200
	}

	if len(taskClassification.DBArtifacts) > 0 || len(taskClassification.StoresArtifacts) > 0 || len(taskClassification.SpacesArtifacts) > 0 {
		profile.ComponentCharBudgets[ComponentSchema] = 0
		profile.ComponentCharBudgets[ComponentHistory] = 1400
	}

	if taskClassification.RoutingGate == RoutingGateContinuity {
		profile.ComponentCharBudgets[ComponentHistory] = 2600
	}

	return profile
}

func summarizePromptBudgetTrim(report PromptBudgetReport) string {
	trimmed := report.TrimmedComponents()
	if len(trimmed) == 0 {
		return ""
	}

	parts := make([]string, 0, len(trimmed))
	for _, stat := range trimmed {
		parts = append(parts, fmt.Sprintf("%s:%d->%d", stat.Component, stat.OriginalChars, stat.FinalChars))
	}
	return strings.Join(parts, ",")
}

func summarizeTaskContextForLog(taskClassification TaskContextClassification) string {
	artifacts := taskClassification.DBArtifacts
	if len(artifacts) == 0 {
		artifacts = append(artifacts, taskClassification.StoresArtifacts...)
		artifacts = append(artifacts, taskClassification.SpacesArtifacts...)
	}
	if len(artifacts) > 4 {
		artifacts = append(append([]string(nil), artifacts[:4]...), "...")
	}
	layers := make([]string, 0, len(taskClassification.Layers))
	for _, layer := range taskClassification.Layers {
		layers = append(layers, fmt.Sprintf("%s[%s]", layer.Name, strings.Join(layer.CRUD, "")))
	}
	return fmt.Sprintf("entity=%s domain=%s artifacts=%s layers=%s", taskClassification.Entity, taskClassification.Domain, strings.Join(artifacts, ","), strings.Join(layers, ","))
}

func (a *CopilotAgent) getSessionMemoryContext() string {
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
					userText := ex.Content
					if len(userText) > 1000 {
						userText = userText[:1000] + "... [truncated for brevity]"
					}
					sb.WriteString(fmt.Sprintf("User: %s\n", userText))
				} else if ex.Role == RoleAssistant {
					astText := ex.Content
					for {
						start := strings.Index(astText, "```json")
						if start == -1 {
							break
						}
						end := strings.Index(astText[start+7:], "```")
						if end == -1 {
							break
						}
						astText = astText[:start] + "```json\n[... AST Script/JSON Payload stripped for brevity ...]\n```" + astText[start+7+end+3:]
					}
					if len(astText) > 800 {
						astText = astText[:800] + "... [Assistant response truncated for brevity]"
					}
					sb.WriteString(fmt.Sprintf("Assistant: %s\n", astText))
				}
			}
			if sb.Len() > 0 {
				convHistory = "\n[ACTIVE SESSION MEMORY]\n" + sb.String() + "\n[/ACTIVE SESSION MEMORY]\n\n"
			}
		}
	}
	return convHistory
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

	var routingState *TaskContextClassification
	if p := ai.GetSessionPayload(ctx); p != nil && p.Variables != nil {
		if rs, ok := p.Variables["RoutingState"].(*TaskContextClassification); ok {
			routingState = rs
		}
	}

	allowedSpacesTools := make(map[string]bool)
	allowedStoresTools := make(map[string]bool)

	if routingState != nil {
		crudParams := collectCRUDFlags(routingState.Layers)
		cross := isCrossDomain(routingState.Layers)

		// Foundational scripts/tools that should always be present
		allowedStoresTools["execute_script"] = true

		if cross || strings.EqualFold(routingState.Domain, SpacesDomain) {
			if crudParams["R"] {
				allowedSpacesTools["read_space_config"] = true
				allowedSpacesTools["search_space"] = true
			}
			if crudParams["C"] || crudParams["U"] {
				allowedSpacesTools["mint_to_space"] = true
				allowedSpacesTools["enrich_space"] = true
				allowedSpacesTools["update_space_config"] = true
				allowedSpacesTools["vectorize_space"] = true
				allowedSpacesTools["vectorize_space_categories"] = true
				allowedSpacesTools["vectorize_space_items"] = true
			}
			if crudParams["D"] {
				allowedSpacesTools["delete_space"] = true
			}
		}

		if cross || strings.EqualFold(routingState.Domain, StoresDomain) {
			if crudParams["R"] {
				allowedStoresTools["select"] = true
				allowedStoresTools["join"] = true
				allowedStoresTools["explain_join"] = true
				allowedStoresTools["scan"] = true
			}
			if crudParams["C"] {
				allowedStoresTools["add"] = true
			}
			if crudParams["U"] {
				allowedStoresTools["update"] = true
			}
			if crudParams["D"] {
				allowedStoresTools["delete"] = true
			}
			allowedStoresTools["manage_transaction"] = true
		}
	}

	isSpaceTool := func(name string) bool {
		switch name {
		case "mint_to_space", "delete_space", "enrich_space", "update_space_config", "read_space_config", "vectorize_space", "vectorize_space_categories", "vectorize_space_items", "search_space":
			return true
		}
		return false
	}

	isStoreTool := func(name string) bool {
		switch name {
		case "select", "join", "explain_join", "add", "update", "delete", "manage_transaction", "scan":
			return true
		}
		return false
	}

	// Append compiled go tools
	if a.registry != nil {
		for _, t := range a.registry.List() {
			if t.Hidden {
				continue // Skip hidden for the LLM natively as well
			}

			// Apply RoutingState Filter if available
			if routingState != nil {
				name := t.Name
				if isSpaceTool(name) && !allowedSpacesTools[name] {
					continue
				}
				if isStoreTool(name) && !allowedStoresTools[name] {
					continue
				}
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
		a.Memory.AgentID = ai.AgentIDOmni
	}

	tx, err := a.systemDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for isolated STM: %w", err)
	}

	var embedder ai.Embeddings
	if a.service != nil && a.service.Domain() != nil && a.service.Domain().Embedder() != nil {
		embedder = a.service.Domain().Embedder()
	}

	// Initialize Memory Unit.
	_, err = a.Memory.OpenShortTermMemory(ctx, a.systemDB, tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("failed to open STM: %w", err)
	}
	_, err = a.Memory.OpenLongTermMemory(ctx, a.systemDB, tx, a.brain, embedder)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("failed to open LTM: %w", err)
	}

	err = tx.Commit(ctx)
	a.Memory.CloseShortTermMemory()
	if err != nil {
		return fmt.Errorf("failed to commit isolated physical memory initialization: %w", err)
	}

	tx, err = a.systemDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed 2nd begin transaction for isolated STM: %w", err)
	}

	log.Info("Initialized Isolated Physical Memory for Avatar", "agent_id", a.Memory.AgentID,
		"stm_store", a.Memory.ShortTermMemoryName(), "ltm_store", a.Memory.LongTermMemoryName())

	// Hook up SOP KB's system tools directly into the Avatar's semantic LTM synchronously
	if embedder != nil {
		alreadySeeded := false
		opts := &memory.SearchOptions[map[string]any]{Limit: 1, CategoryPath: "System_Tools"}
		dummyVec := make([]float32, embedder.Dim())
		ltm, err := a.Memory.OpenLongTermMemory(ctx, a.systemDB, tx, a.brain, embedder)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("failed to open LTM: %w", err)
		}

		if hits, err := ltm.Store.Query(ctx, dummyVec, opts); err == nil && len(hits) > 0 {
			alreadySeeded = true
		}

		if !alreadySeeded {
			sopKB, err := a.systemDB.OpenKnowledgeBase(ctx, ai.DefaultKBName, tx, a.brain, embedder, false)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			config, err := sopKB.GetConfig(ctx)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}

			var toolThoughts []memory.Thought[map[string]any]
			seen := make(map[sop.UUID]bool)

			//opts := &memory.SearchOptions[map[string]any]{Limit: 2, CategoryPath: toolQueries[i].Category}
			result, err := sopKB.SearchByPath(ctx, config.ToolQueries)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}
			for _, res := range result {
				if !seen[res.ID] {
					seen[res.ID] = true
					toolThoughts = append(toolThoughts, memory.Thought[map[string]any]{
						Summaries:    []string{"System Tool Semantic Interface"},
						CategoryPath: "System_Tools",
						Data:         res.Data,
					})
				}
			}

			if len(toolThoughts) > 0 {
				if err := ltm.IngestThoughts(ctx, toolThoughts, a.Memory.AgentID); err != nil {
					tx.Rollback(ctx)
					return err
				}
			}
		}
	}

	err = tx.Commit(ctx)
	a.Memory.CloseShortTermMemory()
	if err != nil {
		return err
	}

	// Wire up Cognitive Memory background workers for the Avatar
	// 1. Batch short-term flushing (Working Memory -> STM)
	a.Memory.StartMemoryWorkers(ctx, a.systemDB)

	// 2. Schedule Sleep Cycle consolidator (STM -> LTM)
	hourlyInterval := a.Config.SleepCycleIntervalHours
	idleTimeout := a.Config.IdleSleepTimeoutMinutes

	a.StartSleepCycle(ctx, hourlyInterval, idleTimeout, nil)

	return nil
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

		kb, err := a.systemDB.OpenKnowledgeBase(embedCtx, kbName, tx, a.brain, embedder, false, true)
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
		log.Info("LLM Tool Call", "tool", toolName)
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
		// Bypass intent routing in unit tests
		return ai.IntentOmni
	}

	p := ai.GetSessionPayload(ctx)

	// 1. Prefix is the overarching router
	parts := strings.Split(query, ":")
	if len(parts) > 1 {
		prefix := strings.ToUpper(strings.TrimSpace(parts[0]))

		if prefix == ai.IntentOmni {
			return ai.IntentOmni
		}

		if p != nil && len(p.SelectedKBs) > 0 {
			for _, kb := range p.SelectedKBs {
				// We compare case-insensitively just in case, but return the canonical KB name
				if prefix == strings.ToUpper(kb.Name) || prefix == kb.Name {
					return kb.Name
				}
			}
		}
	}

	// 2. Without prefix, fall back to explicit UI selected state
	if p != nil {
		if len(p.SelectedKBs) == 1 {
			// Exactly one KB selected -> route to that Avatar
			return p.SelectedKBs[0].Name
		}
		// len(p.SelectedKBs) > 1 -> Future-proofing: multiple KBs selected means we need Omni's cross-KB capabilities
		// len(p.SelectedKBs) == 0 -> No explicit scope, default to Omni
	}

	// 3. Final Default
	return ai.IntentOmni
}
