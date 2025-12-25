package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"

	log "log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/search"
)

// Service is a generic agent service that operates on any Domain.
type Service struct {
	domain            ai.Domain[map[string]any]
	systemDB          *database.Database
	generator         ai.Generator // The LLM (Gemini, etc.)
	pipeline          []PipelineStep
	registry          map[string]ai.Agent[map[string]any]
	EnableObfuscation bool

	// Macro Support
	recording     bool
	recordingMode string // "standard" or "compiled"
	stopOnError   bool
	currentMacro  *ai.Macro
	recordingTx   sop.Transaction
	recordingVars map[string]any
	lastStep      *ai.MacroStep
}

// NewService creates a new agent service for a specific domain.
func NewService(domain ai.Domain[map[string]any], systemDB *database.Database, generator ai.Generator, pipeline []PipelineStep, registry map[string]ai.Agent[map[string]any], enableObfuscation bool) *Service {
	return &Service{
		domain:            domain,
		systemDB:          systemDB,
		generator:         generator,
		pipeline:          pipeline,
		registry:          registry,
		EnableObfuscation: enableObfuscation,
		recordingVars:     make(map[string]any),
	}
}

// Open initializes the agent service.
func (s *Service) Open(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return nil
	}

	// If we are recording and have an active recording transaction, use it.
	if s.recording && s.recordingTx != nil {
		p.Transaction = s.recordingTx
		// Restore variables from recording session
		if p.Variables == nil {
			p.Variables = make(map[string]any)
		}
		for k, v := range s.recordingVars {
			p.Variables[k] = v
		}
		return nil
	}

	// If CurrentDB is a *database.Database, start a transaction
	if db, ok := p.CurrentDB.(*database.Database); ok {
		// We use ForWriting to allow updates by default in a session
		tx, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		p.Transaction = tx
	}
	return nil
}

// Close cleans up the agent service.
func (s *Service) Close(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)
	if p == nil || p.Transaction == nil {
		// If recording, and transaction is gone (e.g. rollback without restart), clear recordingTx
		if s.recording {
			s.recordingTx = nil
			s.recordingVars = nil
		}
		return nil
	}
	if tx, ok := p.Transaction.(sop.Transaction); ok {
		// If we are recording, we capture whatever transaction is currently active
		// as the "recording transaction" for the next request.
		// We do NOT commit it here.
		if s.recording {
			s.recordingTx = tx
			// Persist variables to recording session
			if s.recordingVars == nil {
				s.recordingVars = make(map[string]any)
			}
			for k, v := range p.Variables {
				s.recordingVars[k] = v
			}
			return nil
		}

		// We commit by default on Close.
		// If an error occurred, the caller should have handled Rollback or we need a way to signal it.
		// For now, we assume success if we reached Close without explicit rollback.
		return tx.Commit(ctx)
	}
	return nil
}

// Domain returns the underlying domain of the service.
func (s *Service) Domain() ai.Domain[map[string]any] {
	return s.domain
}

// StopOnError returns true if the agent is configured to stop recording on error.
func (s *Service) StopOnError() bool {
	return s.stopOnError
}

// StopRecording stops the current recording session.
func (s *Service) StopRecording() {
	s.recording = false
	s.currentMacro = nil
	s.recordingTx = nil
}

func (s *Service) getMacroDB() *database.Database {
	return s.systemDB
}

// evaluateInputPolicy checks the input against the domain's policies.
func (s *Service) evaluateInputPolicy(ctx context.Context, input string) error {
	if pol := s.domain.Policies(); pol != nil {
		classifier := s.domain.Classifier()
		if classifier != nil {
			sample := ai.ContentSample{Text: input}
			labels, err := classifier.Classify(ctx, sample)
			if err != nil {
				return fmt.Errorf("classification failed: %w", err)
			}
			decision, err := pol.Evaluate(ctx, "input", sample, labels)
			if err != nil {
				return fmt.Errorf("policy evaluation failed: %w", err)
			}
			if decision.Action == "block" {
				return fmt.Errorf("request blocked by policy: %v", decision.Reasons)
			}
		}
	}
	return nil
}

// Search performs a semantic search in the domain's knowledge base.
// It enforces policies and uses the domain's embedder.
func (s *Service) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	// 1. Policy Check (Input)
	if err := s.evaluateInputPolicy(ctx, query); err != nil {
		return nil, err
	}

	// 2. Embed
	emb := s.domain.Embedder()
	if emb == nil {
		return nil, fmt.Errorf("domain %s has no embedder configured", s.domain.ID())
	}
	vecs, err := emb.EmbedTexts(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("embedding failed: %w", err)
	}

	// 3. Query Index
	tx, err := s.domain.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	idx, err := s.domain.Index(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("domain %s has no index configured: %w", s.domain.ID(), err)
	}

	// Vector Search
	vectorHits, err := idx.Query(ctx, vecs[0], limit, nil)
	if err != nil {
		return nil, fmt.Errorf("vector query failed: %w", err)
	}

	// Text Search
	textIdx, err := s.domain.TextIndex(ctx, tx)
	var textHits []search.TextSearchResult
	if err == nil && textIdx != nil {
		textHits, err = textIdx.Search(ctx, query)
		if err != nil {
			// Log error but continue with vector results?
			// For now, let's treat it as non-fatal if text index is missing or fails,
			// but maybe we should log it.
			// fmt.Printf("Text search failed: %v\n", err)
		}
	}

	// Hybrid Fusion (RRF)
	k := 60.0
	scores := make(map[string]float64)
	payloads := make(map[string]map[string]any)

	// Process Vector Hits
	for rank, hit := range vectorHits {
		scores[hit.ID] += 1.0 / (k + float64(rank+1))
		payloads[hit.ID] = hit.Payload
	}

	// Process Text Hits
	for rank, hit := range textHits {
		scores[hit.DocID] += 1.0 / (k + float64(rank+1))
		// If payload missing, we need to fetch it
		if _, ok := payloads[hit.DocID]; !ok {
			item, err := idx.Get(ctx, hit.DocID)
			if err == nil && item != nil {
				payloads[hit.DocID] = item.Payload
			}
		}
	}

	// Construct Final Results
	var results []ai.Hit[map[string]any]
	for id, score := range scores {
		if payload, ok := payloads[id]; ok {
			results = append(results, ai.Hit[map[string]any]{
				ID:      id,
				Score:   float32(score),
				Payload: payload,
			})
		}
	}

	// Sort by Score Descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// Limit
	if len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// RunPipeline executes the configured chain of agents.
func (s *Service) RunPipeline(ctx context.Context, input string) (string, error) {
	// Note: We do NOT call evaluateInputPolicy here anymore.
	// Policies should be explicitly added as steps in the pipeline if desired.
	// This allows for more flexible policy application (e.g. input, output, intermediate).

	currentInput := input

	for _, step := range s.pipeline {
		agent, ok := s.registry[step.Agent.ID]
		if !ok {
			return "", fmt.Errorf("pipeline agent '%s' not found in registry", step.Agent.ID)
		}

		output, err := agent.Ask(ctx, currentInput)
		if err != nil {
			return "", fmt.Errorf("pipeline step '%s' failed: %w", step.Agent.ID, err)
		}

		if step.OutputTo == "context" {
			// Append context to the input for the next step so it's available
			currentInput = fmt.Sprintf("%s\n\nContext from %s:\n%s", currentInput, step.Agent.ID, output)
		} else {
			// Default or "next_step": The output becomes the input for the next agent
			currentInput = output
		}
	}
	return currentInput, nil
}

// Ask performs a RAG (Retrieval-Augmented Generation) request.
// RecordStep implements the MacroRecorder interface
func (s *Service) RecordStep(step ai.MacroStep) {
	// Always capture the last step for potential manual addition
	s.lastStep = &step

	if s.recording && s.currentMacro != nil {
		// In standard mode, we only record high-level "ask" steps (user intent).
		// We ignore "command" steps (tool calls) because replaying the "ask" step
		// will naturally trigger the tool call again.
		if s.recordingMode == "standard" && step.Type == "command" {
			return
		}
		s.currentMacro.Steps = append(s.currentMacro.Steps, step)
	}
}

func (s *Service) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	cfg := ai.NewAskConfig(opts...)
	var db *database.Database
	if val, ok := cfg.Values["database"]; ok {
		if d, ok := val.(*database.Database); ok {
			db = d
		}
	}

	// Inject SessionPayload into context if present
	if val, ok := cfg.Values["payload"]; ok {
		if p, ok := val.(*ai.SessionPayload); ok {
			ctx = context.WithValue(ctx, "session_payload", p)
			// Also set db from payload if not already set
			if db == nil && p.CurrentDB != nil {
				if d, ok := p.CurrentDB.(*database.Database); ok {
					db = d
				}
			}
		}
	}

	// Inject MacroRecorder into context
	ctx = context.WithValue(ctx, ai.CtxKeyMacroRecorder, s)

	// Handle Macro Management Commands
	if strings.HasPrefix(query, "/macro ") {
		return s.handleMacroCommand(ctx, query)
	}

	// Capture "ask" step for potential manual addition
	// We do this BEFORE handling /record or /play so those commands themselves aren't captured as "ask" steps
	// Wait, /record and /play ARE commands.
	// But we want to capture the LAST "ask" step that was a real interaction.
	// If the user types "Select * from users", that's an "ask".
	// If the user types "/macro step add ...", that's a command.
	// We should only capture if it's NOT a slash command?
	// Or maybe we capture everything, but the user is responsible for running the "ask" first.
	if !strings.HasPrefix(query, "/") {
		s.RecordStep(ai.MacroStep{
			Type:   "ask",
			Prompt: query,
		})
	}

	// Handle Macro Commands
	if strings.HasPrefix(query, "/record ") {
		args := strings.Fields(strings.TrimPrefix(query, "/record "))
		if len(args) == 0 {
			return "Error: Macro name required", nil
		}

		mode := "standard"
		stopOnError := false
		force := false

		// Parse arguments
		// /record [compiled] <name> [--stop-on-error] [--force]
		var cleanArgs []string
		for _, arg := range args {
			if arg == "--stop-on-error" {
				stopOnError = true
			} else if arg == "--force" {
				force = true
			} else {
				cleanArgs = append(cleanArgs, arg)
			}
		}
		args = cleanArgs

		if len(args) == 0 {
			return "Error: Macro name required", nil
		}

		name := args[0]

		if args[0] == "compiled" {
			if len(args) < 2 {
				return "Error: Macro name required for compiled mode", nil
			}
			mode = "compiled"
			name = args[1]
		}

		// Check if macro exists
		macroDB := s.getMacroDB()
		if macroDB != nil {
			tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
			if err == nil {
				store, err := macroDB.OpenModelStore(ctx, "macros", tx)
				if err == nil {
					var dummy ai.Macro
					if err := store.Load(ctx, "macros", name, &dummy); err == nil {
						// Found!
						if !force {
							tx.Rollback(ctx)
							return fmt.Sprintf("Error: Macro '%s' already exists. Use '/record %s --force' to overwrite.", name, name), nil
						}
					}
				}
				tx.Commit(ctx)
			}
		}

		s.recording = true
		s.recordingMode = mode
		s.stopOnError = stopOnError
		// Set macro.Database to current DB if available, else leave empty for composability
		var dbName string
		if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != nil {
			dbName = p.Variables["database"].(string)
			// if d, ok := p.CurrentDB.(*database.Database); ok {
			// 	dbName = d.Name
			// }
		}

		log.Debug(fmt.Sprintf("database: %s", dbName))

		s.currentMacro = &ai.Macro{
			Name:     name,
			Database: dbName,
			Steps:    []ai.MacroStep{},
		}

		// Start a long-running transaction for the recording session
		if db != nil {
			tx, err := db.BeginTransaction(ctx, sop.ForWriting)
			if err == nil {
				s.recordingTx = tx
			} else {
				return fmt.Sprintf("Error starting recording transaction: %v", err), nil
			}
		}

		msg := fmt.Sprintf("Recording macro '%s' (Mode: %s)", name, mode)
		if stopOnError {
			msg += " [Stop on Error]"
		}
		return msg + "...", nil
	}

	if query == "/pause" {
		if s.currentMacro == nil {
			return "Error: No active macro recording", nil
		}
		s.recording = false
		return "Recording paused.", nil
	}

	if query == "/resume" {
		if s.currentMacro == nil {
			return "Error: No active macro recording", nil
		}
		s.recording = true
		return "Recording resumed.", nil
	}

	if query == "/stop" {
		if s.currentMacro == nil {
			return "Error: Not recording", nil
		}
		s.recording = false
		macroDB := s.getMacroDB()
		if macroDB != nil {
			tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Sprintf("Error starting transaction: %v", err), nil
			}
			store, err := macroDB.OpenModelStore(ctx, "macros", tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error opening store: %v", err), nil
			}

			log.Debug(fmt.Sprintf("saving macro w/ db: %s", s.currentMacro.Database))

			if err := store.Save(ctx, "macros", s.currentMacro.Name, s.currentMacro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error saving macro: %v", err), nil
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Sprintf("Error committing transaction: %v", err), nil
			}
			msg := fmt.Sprintf("Macro '%s' saved with %d steps.", s.currentMacro.Name, len(s.currentMacro.Steps))
			s.currentMacro = nil
			return msg, nil
		}
		s.currentMacro = nil
		return "Warning: No database configured, macro lost.", nil
	}

	if strings.HasPrefix(query, "/play ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/play "))
		if len(parts) == 0 {
			return "Error: Macro name required", nil
		}
		name := parts[0]
		args := make(map[string]string)
		for _, arg := range parts[1:] {
			kv := strings.SplitN(arg, "=", 2)
			if len(kv) == 2 {
				args[kv[0]] = kv[1]
			}
		}

		macroDB := s.getMacroDB()
		if macroDB == nil {
			return "Error: No database configured", nil
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), nil
		}

		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), nil
		}

		var macro ai.Macro
		if err := store.Load(ctx, "macros", name, &macro); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error loading macro: %v", err), nil
		}
		tx.Commit(ctx)

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Playing macro '%s'...\n", name))

		// Convert args to map[string]any
		scope := make(map[string]any)
		for k, v := range args {
			scope[k] = v
		}
		var scopeMu sync.RWMutex

		// Lifecycle Management for Macro Execution
		if err := s.Open(ctx); err != nil {
			return fmt.Sprintf("Error initializing session: %v", err), nil
		}
		// Ensure we close the session (commit transaction)
		defer func() {
			if err := s.Close(ctx); err != nil {
				sb.WriteString(fmt.Sprintf("\nError closing session: %v", err))
			}
		}()

		if err := s.executeMacro(ctx, macro.Steps, scope, &scopeMu, &sb, db); err != nil {
			sb.WriteString(fmt.Sprintf("Error executing macro: %v\n", err))
			// If error, we might want to rollback.
			// Currently Close() commits.
			// We should probably rollback here if we can access the transaction.
			if p := ai.GetSessionPayload(ctx); p != nil && p.Transaction != nil {
				if tx, ok := p.Transaction.(sop.Transaction); ok {
					tx.Rollback(ctx)
					p.Transaction = nil // Prevent Close from committing
				}
			}
		}
		return sb.String(), nil
	}

	if strings.HasPrefix(query, "/delete ") {
		name := strings.TrimPrefix(query, "/delete ")
		if name == "" {
			return "Error: Macro name required", nil
		}

		macroDB := s.getMacroDB()
		if macroDB == nil {
			return "Error: No database configured", nil
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), nil
		}

		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), nil
		}

		if err := store.Delete(ctx, "macros", name); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error deleting macro: %v", err), nil
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Sprintf("Error committing transaction: %v", err), nil
		}
		return fmt.Sprintf("Macro '%s' deleted.", name), nil
	}

	if query == "/list" {
		macroDB := s.getMacroDB()
		if macroDB == nil {
			return "Error: No database configured", nil
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), nil
		}

		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), nil
		}

		names, err := store.List(ctx, "macros")
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error listing macros: %v", err), nil
		}
		tx.Commit(ctx)

		var sb strings.Builder
		sb.WriteString("Available Macros:\n")
		for _, n := range names {
			sb.WriteString(fmt.Sprintf("- %s\n", n))
		}
		return sb.String(), nil
	}

	// Record step if recording
	if s.recording {
		// Only record "ask" step if NOT in compiled mode
		if s.recordingMode != "compiled" {
			s.currentMacro.Steps = append(s.currentMacro.Steps, ai.MacroStep{
				Type:   "ask",
				Prompt: query,
			})
		}
	}

	// Obfuscate Input
	// We ONLY obfuscate known resource names (Database, Store) that have been registered.
	// We do NOT obfuscate the entire text blindly, but ObfuscateText only replaces known keys.
	// If the user says "select from Python Complex DB", and "Python Complex DB" is registered,
	// it becomes "select from DB_123". This is correct.
	// The LLM sees "select from DB_123".

	// Obfuscate Query if enabled
	if s.EnableObfuscation {
		query = obfuscation.GlobalObfuscator.ObfuscateText(query)
	}

	// 0. Pipeline Execution (if configured)
	if len(s.pipeline) > 0 {
		resp, err := s.RunPipeline(ctx, query)
		// Update recordingTx if the pipeline changed the transaction (e.g. via manage_transaction tool)
		if s.recording {
			if p := ai.GetSessionPayload(ctx); p != nil && p.Transaction != nil {
				if tx, ok := p.Transaction.(sop.Transaction); ok {
					s.recordingTx = tx
				}
			}
		}
		return resp, err
	}

	// 1. Search for context
	hits, err := s.Search(ctx, query, 10)
	if err != nil {
		return "", fmt.Errorf("retrieval failed: %w", err)
	}

	// 2. Construct Prompt
	contextText := s.formatContext(hits)
	systemPrompt, _ := s.domain.Prompt(ctx, "system")

	// If obfuscation is enabled, we should obfuscate the context too.
	// This ensures that if the vector store returns real names, they are hidden from the LLM.
	if s.EnableObfuscation {
		contextText = obfuscation.GlobalObfuscator.ObfuscateText(contextText)
	}

	fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s\n\nUser Query: %s", systemPrompt, contextText, query)
	if s.EnableObfuscation {
	}

	// 3. Determine Generator
	gen := s.generator

	// Check for override in context
	if provider, ok := ctx.Value(ai.CtxKeyProvider).(string); ok && provider != "" {
		// Only override if the requested provider is different from the current one
		// (We assume s.generator.Name() matches the provider string, e.g. "gemini", "ollama")
		if gen == nil || gen.Name() != provider {
			// Create a temporary generator instance
			// We rely on the generator package to pick up API keys from Env Vars
			overriddenGen, err := generator.New(provider, nil)
			if err == nil {
				gen = overriddenGen
			} else {
				// Log warning? For now, just fall back to default
				fmt.Printf("Warning: Failed to initialize requested provider '%s': %v. Falling back to default.\n", provider, err)
			}
		}
	}

	// 4. Generate Answer
	if gen == nil {
		// Fallback: If no generator is configured, return the retrieved context directly.
		// This allows agents to act as "Search Services" or "Translators" without an LLM.
		if s.EnableObfuscation {
			return obfuscation.GlobalObfuscator.DeobfuscateText(contextText), nil
		}
		return contextText, nil
	}

	output, err := gen.Generate(ctx, fullPrompt, ai.GenOptions{
		MaxTokens:   1024,
		Temperature: 0.7,
	})
	if err != nil {
		return "", fmt.Errorf("generation failed: %w", err)
	}
	if s.EnableObfuscation {
	}

	// Track if we recorded a tool call to avoid duplicate "print" recording
	toolRecorded := false

	// Check for Raw Tool Call (from DataAdmin or similar generators)
	if output.Raw != nil {
		if b, err := json.Marshal(output.Raw); err == nil {
			if s.recording {
				s.currentMacro.Steps = append(s.currentMacro.Steps, ai.MacroStep{
					Type:   "ask",
					Prompt: string(b), // Store raw tool call as prompt for now? Or maybe a new "tool" type?
					// For now, let's use "ask" but maybe we need a "tool" type in the schema?
					// The user didn't specify "tool" in the new schema.
					// Let's assume "ask" covers it or we just store it as prompt.
				})
			}
			toolRecorded = true
		}
	}

	// 5. Check for Tool Execution (Agent -> App)
	// If the generator returns a JSON tool call, and we have an executor, run it.
	if !toolRecorded {
		if executor, ok := ctx.Value(ai.CtxKeyExecutor).(ai.ToolExecutor); ok && executor != nil {
			// Simple heuristic: If output looks like a JSON tool call
			text := strings.TrimSpace(output.Text)
			// Remove markdown code blocks if present
			text = strings.TrimPrefix(text, "```json")
			text = strings.TrimPrefix(text, "```")
			text = strings.TrimSuffix(text, "```")
			text = strings.TrimSpace(text)

			if strings.HasPrefix(text, "{") && strings.Contains(text, "\"tool\"") {
				// De-obfuscate Tool Arguments
				// We need to parse, de-obfuscate, and re-serialize (or just return the de-obfuscated JSON)
				// Since we return text, we should return the de-obfuscated JSON so the caller can execute it.

				// 1. Parse JSON FIRST to get the exact values the LLM returned
				var toolCall struct {
					Tool string         `json:"tool"`
					Args map[string]any `json:"args"`
				}

				// We try to unmarshal the text directly.
				// If the LLM returned valid JSON (even with obfuscated values), this will succeed.
				if err := json.Unmarshal([]byte(text), &toolCall); err == nil {
					// 2. Sanitize Args
					for k, v := range toolCall.Args {
						if val, ok := v.(string); ok {
							// a. Remove Markdown bold/italics/code wrappers from the value itself
							// LLM might return: "database": "**DB_123**" or "`DB_123`"
							val = strings.Trim(val, "*_`")

							// b. Replace NBSP with space and Trim whitespace
							val = strings.ReplaceAll(val, "\u00a0", " ")
							val = strings.TrimSpace(val)

							// c. De-obfuscate if enabled
							if s.EnableObfuscation {
								val = obfuscation.GlobalObfuscator.DeobfuscateText(val)
							}

							toolCall.Args[k] = val
						}
					}

					// Inject Database from Options if missing
					if db != nil {
						// We can't inject the *database.Database object into the JSON args directly
						// because it's not serializable.
						// However, the ToolExecutor might need it.
						// The DefaultToolExecutor in main.ai.go doesn't use the DB object from args,
						// it uses the DB name string to open it.
						// But wait, we just refactored main.ai.go to NOT put DB in context.
						// And DefaultToolExecutor executes tools.
						// If the tool is "select", it needs a DB.
						// The LLM returns the DB name (obfuscated or not).
						// If we want to support "contextual DB", we might need to pass the DB object to the executor.
						// But ToolExecutor.Execute takes map[string]any.
						// We can put the DB object in there!
						toolCall.Args["_db_instance"] = db
					}

					// Re-serialize
					if b, err := json.Marshal(toolCall); err == nil {
						cleanJSON := string(b)

						// Always capture as last step (for manual addition)
						s.lastStep = &ai.MacroStep{
							Type:    "command",
							Command: toolCall.Tool,
							Args:    toolCall.Args,
						}

						// Record Tool Call if recording
						if s.recording {
							if s.recordingMode == "compiled" {
								s.currentMacro.Steps = append(s.currentMacro.Steps, *s.lastStep)
							} else {
								s.currentMacro.Steps = append(s.currentMacro.Steps, ai.MacroStep{
									Type:   "ask",
									Prompt: cleanJSON,
								})
							}
						}

						// Update recordingTx if the tool changed the transaction (e.g. via manage_transaction tool)
						// Note: This only works if the tool was executed by the generator/pipeline and updated the payload.
						// If the tool is executed by the caller (main.ai.go), we can't see the update here.
						// But wait, Service.Ask returns the JSON string, and main.ai.go executes it.
						// So Service doesn't know if the transaction changed!
						// However, main.ai.go calls executeTool.
						// executeTool updates the payload in context.
						// But context values are immutable?
						// No, SessionPayload is a pointer!
						// So if main.ai.go updates payload.Transaction, Service can see it if it holds the same payload pointer.
						// Service.Ask gets the payload from context.
						// So yes, we can check it here?
						// No, Service.Ask returns BEFORE main.ai.go executes the tool.
						// So Service cannot update recordingTx here.
						// BUT, Service.Open is called at the START of the request.
						// If main.ai.go executes the tool, the transaction changes for the NEXT request.
						// So when the user sends the NEXT message, Service.Open will be called.
						// Service.Open checks s.recordingTx.
						// If s.recordingTx is stale (committed), we have a problem.
						// But wait, if main.ai.go updates payload.Transaction to a NEW transaction (via auto-begin),
						// then in the NEXT request, payload.Transaction will be... wait.
						// SessionPayload is recreated per request in main.ai.go?
						// Let's check main.ai.go.

						return cleanJSON, nil
					}
				}

				// Fallback: If JSON parsing failed (maybe invalid JSON), return as is
				return text, nil
			}
		}
	}

	// De-obfuscate Output Text
	finalText := output.Text
	if s.EnableObfuscation {
		finalText = obfuscation.GlobalObfuscator.DeobfuscateText(output.Text)
	}

	// Record Chat Output if recording
	if s.recording && !toolRecorded {
		s.currentMacro.Steps = append(s.currentMacro.Steps, ai.MacroStep{
			Type:    "say",
			Message: finalText,
		})
	}

	// Always capture text response as last step if not a tool call
	if !toolRecorded {
		s.lastStep = &ai.MacroStep{
			Type:    "say",
			Message: finalText,
		}
	}

	return finalText, nil
}

func (s *Service) formatContext(hits []ai.Hit[map[string]any]) string {
	var sb strings.Builder
	for i, hit := range hits {
		// Generic handling of metadata
		sb.WriteString(fmt.Sprintf("[%d] ", i+1))

		text, hasText := hit.Payload["text"].(string)
		desc, hasDesc := hit.Payload["description"].(string)

		if hasText && hasDesc {
			sb.WriteString(fmt.Sprintf("%s: %s (Score: %.2f)", text, desc, hit.Score))
		} else if hasDesc {
			sb.WriteString(fmt.Sprintf("%s (Score: %.2f)", desc, hit.Score))
		} else if hasText {
			sb.WriteString(fmt.Sprintf("%s (Score: %.2f)", text, hit.Score))
		} else {
			sb.WriteString(fmt.Sprintf("%v (Score: %.2f)", hit.Payload, hit.Score))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// executeMacro runs a sequence of macro steps with support for control flow and variables.
func (s *Service) executeMacro(ctx context.Context, steps []ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	// Create cancellable context for this macro execution scope to support stopping on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Use errgroup for managing concurrency and error propagation
	g, groupCtx := errgroup.WithContext(ctx)

	var syncErr error

	for _, step := range steps {
		// Check if context is already cancelled (by errgroup or sync error)
		if groupCtx.Err() != nil {
			break
		}

		// Refresh DB from payload to ensure we use the current active database
		// This handles cases where a previous step (e.g. a tool) switched the database.
		if p := ai.GetSessionPayload(groupCtx); p != nil{
			d := p.GetDatabase()
			if d != nil {
				db = d.(*database.Database)
			}
		}
		log.Debug(fmt.Sprintf("DB from payload: %v", db))

		// If async, run in errgroup
		if step.IsAsync {
			// Check if we are in a transaction.
			// If so, we generally force sync execution to maintain integrity for simple steps.
			// HOWEVER, if the step is a nested "macro", we allow it to run async (Swarm pattern),
			// assuming it will manage its own transaction (e.g. on a different DB).
			p := ai.GetSessionPayload(groupCtx)
			if p != nil && p.Transaction != nil && step.Type != "macro" {
				msg := fmt.Sprintf("Info: Step '%s' marked async but active transaction exists. Running synchronously.\n", step.Type)
				sb.WriteString(msg)
				if w, ok := ctx.Value(ai.CtxKeyWriter).(io.Writer); ok && w != nil {
					fmt.Fprint(w, msg)
				}
				// Fall through to sync execution
			} else {
				// Detach transaction to enforce isolation (Swarm Computing model)
				// Async steps must not share the parent transaction to avoid race conditions.
				// They should start their own transaction if needed.
				asyncCtx := groupCtx
				if p != nil {
					// Clone payload and remove transaction (though p.Transaction should be nil here based on check above,
					// but good to be safe if logic changes)
					newPayload := *p
					newPayload.Transaction = nil
					asyncCtx = context.WithValue(groupCtx, "session_payload", &newPayload)
				}

				// Capture step for closure
				st := step
				g.Go(func() error {
					// Handle panic in async step
					defer func() {
						if r := recover(); r != nil {
							// We can't easily return the panic as error here without named return,
							// but errgroup expects an error return.
							// We'll just log it (if we had a logger) and return a generic error.
							// For now, let's assume runStep handles its own panics or we let it crash?
							// Better to recover and return error to stop the group.
						}
					}()

					if err := s.runStep(asyncCtx, st, scope, scopeMu, sb, db); err != nil {
						if st.ContinueOnError {
							// Log error but don't stop the group
							// TODO: Add logging
							return nil
						}
						return err // This cancels groupCtx
					}
					return nil
				})
				continue
			}
		}

		// Sync step
		if err := s.runStep(groupCtx, step, scope, scopeMu, sb, db); err != nil {
			if step.ContinueOnError {
				// Log error and continue
				continue
			}
			// Stop everything
			syncErr = err
			cancel() // Cancel context for async tasks
			break
		}
	}

	// Wait for all async tasks
	asyncErr := g.Wait()

	if syncErr != nil {
		return syncErr
	}
	return asyncErr
}

func (s *Service) resolveTemplate(tmplStr string, scope map[string]any, scopeMu *sync.RWMutex) string {
	if tmplStr == "" {
		return ""
	}

	if scopeMu != nil {
		scopeMu.RLock()
		defer scopeMu.RUnlock()
	}

	if tmpl, err := template.New("tmpl").Parse(tmplStr); err == nil {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, scope); err == nil {
			return buf.String()
		}
	}
	return tmplStr
}

func (s *Service) handleMacroCommand(ctx context.Context, query string) (string, error) {
	args := strings.Fields(strings.TrimPrefix(query, "/macro "))
	if len(args) == 0 {
		return "Usage: /macro <list|show|delete|step> ...", nil
	}

	cmd := args[0]
	macroDB := s.getMacroDB()
	if macroDB == nil {
		return "Error: No database configured", nil
	}

	switch cmd {
	case "list":
		tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), nil
		}
		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), nil
		}
		names, err := store.List(ctx, "macros")
		tx.Commit(ctx)
		if err != nil {
			return fmt.Sprintf("Error listing macros: %v", err), nil
		}
		if len(names) == 0 {
			return "No macros found.", nil
		}
		return "Macros:\n- " + strings.Join(names, "\n- "), nil

	case "show":
		if len(args) < 2 {
			return "Usage: /macro show <name> [--json]", nil
		}
		name := args[1]
		showJSON := false
		if len(args) > 2 && args[2] == "--json" {
			showJSON = true
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), nil
		}
		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), nil
		}
		var macro ai.Macro
		err = store.Load(ctx, "macros", name, &macro)
		tx.Commit(ctx)
		if err != nil {
			return fmt.Sprintf("Error loading macro: %v", err), nil
		}

		if showJSON {
			b, err := json.MarshalIndent(macro, "", "  ")
			if err != nil {
				return fmt.Sprintf("Error marshaling macro: %v", err), nil
			}
			return fmt.Sprintf("```json\n%s\n```", string(b)), nil
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Macro: %s\n", macro.Name))
		for i, step := range macro.Steps {
			desc := step.Message
			if step.Type == "ask" {
				desc = step.Prompt
			} else if step.Type == "macro" {
				desc = fmt.Sprintf("Run '%s'", step.MacroName)
			} else if step.Type == "command" {
				argsJSON, _ := json.Marshal(step.Args)
				desc = fmt.Sprintf("Execute '%s' %s", step.Command, string(argsJSON))
			}
			sb.WriteString(fmt.Sprintf("%d. [%s] %s", i+1, step.Type, desc))
			sb.WriteString("\n")
		}
		return sb.String(), nil

	case "delete":
		if len(args) < 2 {
			return "Usage: /macro delete <name>", nil
		}
		name := args[1]
		tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), nil
		}
		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), nil
		}
		err = store.Delete(ctx, "macros", name)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error deleting macro: %v", err), nil
		}
		tx.Commit(ctx)
		return fmt.Sprintf("Macro '%s' deleted.", name), nil

	case "save_as":
		if len(args) < 2 {
			return "Usage: /macro save_as <name>", nil
		}
		name := args[1]
		if s.lastStep == nil {
			return "Error: No previous step available to save. Run a command first.", nil
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), nil
		}
		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), nil
		}

		// Check if macro exists
		var dummy ai.Macro
		if err := store.Load(ctx, "macros", name, &dummy); err == nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error: Macro '%s' already exists. Use '/macro delete %s' first.", name, name), nil
		}

		newMacro := ai.Macro{
			Name:  name,
			Steps: []ai.MacroStep{*s.lastStep},
		}

		if err := store.Save(ctx, "macros", name, newMacro); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error saving macro: %v", err), nil
		}
		tx.Commit(ctx)
		return fmt.Sprintf("Macro '%s' created from last step.", name), nil

	case "step":
		if len(args) < 3 {
			return "Usage: /macro step <add|delete> <macro_name> ...", nil
		}
		subCmd := args[1]
		name := args[2]

		if subCmd == "add" {
			if len(args) < 4 {
				return "Usage: /macro step add <macro_name> <top|bottom>", nil
			}
			position := args[3]
			if position != "top" && position != "bottom" {
				return "Error: Position must be 'top' or 'bottom'", nil
			}

			if s.lastStep == nil {
				return "Error: No previous step available to add. Run a command first.", nil
			}

			tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Sprintf("Error starting transaction: %v", err), nil
			}
			store, err := macroDB.OpenModelStore(ctx, "macros", tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error opening store: %v", err), nil
			}
			var macro ai.Macro
			if err := store.Load(ctx, "macros", name, &macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error loading macro: %v", err), nil
			}

			if position == "top" {
				macro.Steps = append([]ai.MacroStep{*s.lastStep}, macro.Steps...)
			} else {
				macro.Steps = append(macro.Steps, *s.lastStep)
			}

			if err := store.Save(ctx, "macros", name, macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error saving macro: %v", err), nil
			}
			tx.Commit(ctx)
			return fmt.Sprintf("Step added to %s of macro '%s'.", position, name), nil
		}

		if subCmd == "delete" {
			if len(args) < 4 {
				return "Usage: /macro step delete <macro_name> <step_index>", nil
			}
			idxStr := args[3]
			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 1 {
				return "Error: Invalid step index", nil
			}
			// Adjust to 0-based
			idx--

			tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Sprintf("Error starting transaction: %v", err), nil
			}
			store, err := macroDB.OpenModelStore(ctx, "macros", tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error opening store: %v", err), nil
			}
			var macro ai.Macro
			if err := store.Load(ctx, "macros", name, &macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error loading macro: %v", err), nil
			}

			if idx >= len(macro.Steps) {
				tx.Rollback(ctx)
				return "Error: Step index out of range", nil
			}

			// Remove step
			macro.Steps = append(macro.Steps[:idx], macro.Steps[idx+1:]...)

			if err := store.Save(ctx, "macros", name, macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error saving macro: %v", err), nil
			}
			tx.Commit(ctx)
			return fmt.Sprintf("Step %d deleted from macro '%s'.", idx+1, name), nil
		}

		if subCmd == "update" {
			// /macro step update <macro_name> <step_index>
			if len(args) < 4 {
				return "Usage: /macro step update <macro_name> <step_index>", nil
			}
			if s.lastStep == nil {
				return "Error: No previous step available to update with. Run a command first.", nil
			}

			idxStr := args[3]
			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 1 {
				return "Error: Invalid step index", nil
			}
			idx-- // 0-based

			tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Sprintf("Error starting transaction: %v", err), nil
			}
			store, err := macroDB.OpenModelStore(ctx, "macros", tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error opening store: %v", err), nil
			}
			var macro ai.Macro
			if err := store.Load(ctx, "macros", name, &macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error loading macro: %v", err), nil
			}

			if idx >= len(macro.Steps) {
				tx.Rollback(ctx)
				return "Error: Step index out of range", nil
			}

			// Update step
			macro.Steps[idx] = *s.lastStep

			if err := store.Save(ctx, "macros", name, macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error saving macro: %v", err), nil
			}
			tx.Commit(ctx)
			return fmt.Sprintf("Step %d updated in macro '%s'.", idx+1, name), nil
		}

		if subCmd == "add" {
			// /macro step add <macro_name> <position> [target_index]
			// position: top, bottom, before, after
			if len(args) < 4 {
				return "Usage: /macro step add <macro_name> <position> [target_index]", nil
			}
			if s.lastStep == nil {
				return "Error: No previous step available to add. Run a command first.", nil
			}

			position := args[3]
			targetIdx := -1
			if position == "before" || position == "after" {
				if len(args) < 5 {
					return "Usage: /macro step add <macro_name> <before|after> <target_index>", nil
				}
				var err error
				targetIdx, err = strconv.Atoi(args[4])
				if err != nil || targetIdx < 1 {
					return "Error: Invalid target index", nil
				}
				targetIdx-- // 0-based
			}

			tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Sprintf("Error starting transaction: %v", err), nil
			}
			store, err := macroDB.OpenModelStore(ctx, "macros", tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error opening store: %v", err), nil
			}
			var macro ai.Macro
			if err := store.Load(ctx, "macros", name, &macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error loading macro: %v", err), nil
			}

			newStep := *s.lastStep

			switch position {
			case "top":
				macro.Steps = append([]ai.MacroStep{newStep}, macro.Steps...)
			case "bottom":
				macro.Steps = append(macro.Steps, newStep)
			case "before":
				if targetIdx >= len(macro.Steps) {
					tx.Rollback(ctx)
					return "Error: Target index out of range", nil
				}
				macro.Steps = append(macro.Steps[:targetIdx], append([]ai.MacroStep{newStep}, macro.Steps[targetIdx:]...)...)
			case "after":
				if targetIdx >= len(macro.Steps) {
					tx.Rollback(ctx)
					return "Error: Target index out of range", nil
				}
				// Insert after targetIdx (so at targetIdx + 1)
				targetIdx++
				macro.Steps = append(macro.Steps[:targetIdx], append([]ai.MacroStep{newStep}, macro.Steps[targetIdx:]...)...)
			default:
				tx.Rollback(ctx)
				return "Error: Invalid position. Use top, bottom, before, or after.", nil
			}

			if err := store.Save(ctx, "macros", name, macro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error saving macro: %v", err), nil
			}
			tx.Commit(ctx)
			return fmt.Sprintf("Step added to macro '%s' at %s.", name, position), nil
		}

		return "Unknown step command. Usage: /macro step <delete|add> ...", nil

	default:
		return "Unknown macro command. Usage: /macro <list|show|delete|step> ...", nil
	}
}
