package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	log "log/slog"

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
	databases         map[string]sop.DatabaseOptions
	generator         ai.Generator // The LLM (Gemini, etc.)
	pipeline          []PipelineStep
	registry          map[string]ai.Agent[map[string]any]
	EnableObfuscation bool

	// Session State
	session *RunnerSession
}

// NewService creates a new agent service for a specific domain.
func NewService(domain ai.Domain[map[string]any], systemDB *database.Database, databases map[string]sop.DatabaseOptions, generator ai.Generator, pipeline []PipelineStep, registry map[string]ai.Agent[map[string]any], enableObfuscation bool) *Service {
	return &Service{
		domain:            domain,
		systemDB:          systemDB,
		databases:         databases,
		generator:         generator,
		pipeline:          pipeline,
		registry:          registry,
		EnableObfuscation: enableObfuscation,
		session:           NewRunnerSession(),
	}
}

// Open initializes the agent service.
func (s *Service) Open(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return nil
	}

	// If we are recording, we do NOT use the session transaction.
	// The user requirement is that during recording, each step is an isolated transaction (auto-commit).
	// Explicit transaction commands (begin/commit) are recorded as steps but do not affect the recording session.
	if s.session.Recording {
		// Ensure we don't accidentally carry over any state
		p.Transaction = nil
	} else if s.session.Transaction != nil {
		// If NOT recording, and we have an active session transaction (e.g. from a previous step in a stateful session), use it.
		// BUT ONLY if it matches the requested database.
		if s.session.CurrentDB == "" || s.session.CurrentDB == p.CurrentDB {
			p.Transaction = s.session.Transaction
			p.Variables = s.session.Variables
			// Restore ExplicitTransaction flag if we are reusing a transaction
			// We assume if s.session.Transaction is set, it was explicit (based on Close logic),
			// but let's be safe. Actually, Close only saves it if it WAS explicit.
			// So we can set it to true here.
			p.ExplicitTransaction = true
			return nil
		}
		// If DB mismatch, we commit the previous transaction as we are switching context.
		if s.session.CurrentDB != "" && s.session.CurrentDB != p.CurrentDB {
			if s.session.Transaction != nil {
				// Commit the old transaction to persist changes
				if err := s.session.Transaction.Commit(ctx); err != nil {
					return fmt.Errorf("failed to commit previous transaction on database '%s' before switching to '%s': %w", s.session.CurrentDB, p.CurrentDB, err)
				}
			}
			// Clear the session transaction as we've committed it
			s.session.Transaction = nil
			s.session.Variables = nil
		}
	}

	if p.CurrentDB == "" {
		return nil
	}

	// Look up the database in the known databases
	if dbOpts, ok := s.databases[p.CurrentDB]; ok {
		db := database.NewDatabase(dbOpts)

		// Start transaction
		tx, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Errorf("failed to begin transaction on database '%s': %w", p.CurrentDB, err)
		}
		p.Transaction = tx
	} else {
		return fmt.Errorf("database '%s' not found in agent configuration", p.CurrentDB)
	}
	return nil
}

// Close cleans up the agent service.
func (s *Service) Close(ctx context.Context) error {
	p := ai.GetSessionPayload(ctx)
	if p == nil || p.Transaction == nil {
		return nil
	}
	if tx, ok := p.Transaction.(sop.Transaction); ok {
		// If we are recording, we do NOT capture the transaction.
		// We commit it immediately (auto-commit per step).
		if !s.session.Recording {
			// If the transaction was explicitly started by the user, we persist it.
			if p.ExplicitTransaction {
				s.session.Transaction = tx
				s.session.CurrentDB = p.CurrentDB
				s.session.Variables = p.Variables
				return nil
			}
			// Otherwise, we commit it as it's an implicit transaction for this request/script.
			if tx.HasBegun() {
				if err := tx.Commit(ctx); err != nil {
					return fmt.Errorf("failed to commit implicit transaction: %w", err)
				}
			}
			// Clear session state
			s.session.Transaction = nil
			s.session.Variables = nil
			return nil
		}

		// We commit by default on Close.
		// If an error occurred, the caller should have handled Rollback or we need a way to signal it.
		// For now, we assume success if we reached Close without explicit rollback.
		if tx.HasBegun() {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("failed to commit transaction on close: %w", err)
			}
		}
		// Ensure we clear the session transaction to prevent reuse
		s.session.Transaction = nil
		s.session.Variables = nil
		return nil
	}
	return nil
}

// Domain returns the underlying domain of the service.
func (s *Service) Domain() ai.Domain[map[string]any] {
	return s.domain
}

// StopOnError returns true if the agent is configured to stop recording on error.
func (s *Service) StopOnError() bool {
	return s.session.StopOnError
}

// StopRecording stops the current recording session.
func (s *Service) StopRecording() {
	s.session.Recording = false
	s.session.CurrentScript = nil
	s.session.Transaction = nil
	s.session.Variables = nil
}

func (s *Service) getScriptDB() *database.Database {
	return s.systemDB
}

// evaluateInputPolicy checks the input against the domain's policies.
func (s *Service) evaluateInputPolicy(ctx context.Context, input string) error {
	if s.domain == nil {
		return nil
	}
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

	if s.domain == nil {
		return nil, nil
	}

	// 2. Embed
	emb := s.domain.Embedder()
	if emb == nil {
		// If no embedder is configured, we cannot perform vector search.
		// Return empty results instead of error, allowing the agent to proceed without context.
		return nil, nil
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
			log.Warn("Text search failed", "error", err)
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

// GetLastToolInstructions returns the JSON instructions of the last executed tool.
func (s *Service) GetLastToolInstructions() string {
	if s.session == nil {
		return ""
	}

	// Try to get the last command from LastInteractionToolCalls if available,
	// as this is the most reliable source for the *last interaction's* tools.
	var targetStep *ai.ScriptStep

	if len(s.session.LastInteractionToolCalls) > 0 {
		// Use the last one in the buffer
		targetStep = &s.session.LastInteractionToolCalls[len(s.session.LastInteractionToolCalls)-1]
	} else if s.session.LastStep != nil && s.session.LastStep.Type == "command" {
		// Fallback to LastStep
		targetStep = s.session.LastStep
	}

	if targetStep == nil || targetStep.Type != "command" {
		return ""
	}

	// Debug: Log what we are retrieving
	if script, ok := targetStep.Args["script"]; ok {
		log.Debug(fmt.Sprintf("Service.GetLastToolInstructions: Retrieving script. Type: %T, Value: %+v", script, script))
	} else {
		log.Debug(fmt.Sprintf("Service.GetLastToolInstructions: Retrieving command '%s' without script. Args keys: %v", targetStep.Command, reflect.ValueOf(targetStep.Args).MapKeys()))
	}

	// Reconstruct the tool call structure
	toolCall := map[string]any{
		"tool": targetStep.Command,
		"args": targetStep.Args,
	}

	b, _ := json.MarshalIndent(toolCall, "", "  ")
	return string(b)
}

// Ask performs a RAG (Retrieval-Augmented Generation) request.
// RecordStep implements the ScriptRecorder interface
func (s *Service) RecordStep(ctx context.Context, step ai.ScriptStep) {
	// Debug: Log what we are recording
	if step.Type == "command" {
		if script, ok := step.Args["script"]; ok {
			log.Debug(fmt.Sprintf("Service.RecordStep: Recording script. Type: %T, Value: %+v", script, script))
		} else {
			log.Debug(fmt.Sprintf("Service.RecordStep: Recording command '%s' without script. Args keys: %v", step.Command, reflect.ValueOf(step.Args).MapKeys()))
		}
	}

	// Deep copy args to ensure we persist the exact state at this moment
	// and protect against future mutations of the map by the caller.
	if step.Args != nil {
		step.Args = deepCopyMap(step.Args)
	}

	// Always capture the last step for potential manual addition
	s.session.LastStep = &step

	// Buffer tool calls for potential refactoring
	if step.Type == "command" {
		s.session.LastInteractionToolCalls = append(s.session.LastInteractionToolCalls, step)
	}

	if s.session.Recording && s.session.CurrentScript != nil {
		// In standard mode, we only record high-level "ask" steps (user intent).
		// We ignore "command" steps (tool calls) because replaying the "ask" step
		// will naturally trigger the tool call again.
		if s.session.RecordingMode == "standard" && step.Type == "command" {
			return
		}
		if err := s.appendStepToCurrentScript(ctx, step); err != nil {
			log.Error("failed to append step to script", "error", err)
		}
	}
}

func (s *Service) appendStepToCurrentScript(ctx context.Context, step ai.ScriptStep) error {
	if !s.session.Recording || s.session.CurrentScript == nil {
		return nil
	}
	scriptDB := s.getScriptDB()
	if scriptDB == nil {
		return nil
	}

	// Use a separate transaction for saving the script
	tx, err := scriptDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("failed to open scripts store: %w", err)
	}

	// Try to load the latest version of the script from disk
	var latestScript ai.Script
	if err := store.Load(ctx, s.session.CurrentScriptCategory, s.session.CurrentScript.Name, &latestScript); err != nil {
		// If load fails, assume it's a new script (first step) and use the session's initial state
		// We copy the metadata from the session script
		latestScript = *s.session.CurrentScript
		// Ensure steps are empty if we are starting fresh (or use what's in session if we trust it)
		// Since we are appending, we assume session might be empty or have previous steps?
		// Actually, if Load fails, it means it's not on disk.
		// So we should use the session's script as the base, but we need to be careful about duplication if session has steps.
		// But in this flow, we only append via this method.
		// So if it's not on disk, session steps should be empty (except for what we are about to add).
		// However, s.session.CurrentScript is a pointer.
	}

	// Append the new step
	latestScript.Steps = append(latestScript.Steps, step)

	// Save back to disk
	if err := store.Save(ctx, s.session.CurrentScriptCategory, latestScript.Name, &latestScript); err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("failed to save script: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Update in-memory session to match disk
	s.session.CurrentScript = &latestScript
	return nil
}

// RefactorLastSteps refactors the last N steps into a new structure (script or block).
func (s *Service) RefactorLastSteps(count int, mode string, name string) error {
	if !s.session.Recording || s.session.CurrentScript == nil {
		return fmt.Errorf("not recording")
	}

	var stepsToGroup []ai.ScriptStep

	if s.session.RecordingMode == "standard" {
		// In standard mode, the last step in CurrentScript is likely the "ask" step.
		// We want to replace it with the buffered tool calls.
		if len(s.session.CurrentScript.Steps) > 0 {
			lastIdx := len(s.session.CurrentScript.Steps) - 1
			if s.session.CurrentScript.Steps[lastIdx].Type == "ask" {
				s.session.CurrentScript.Steps = s.session.CurrentScript.Steps[:lastIdx]
			}
		}
		stepsToGroup = s.session.LastInteractionToolCalls
	} else {
		// In compiled mode, the tool calls are already in CurrentScript.Steps.
		// Use count if provided, otherwise use the length of buffered tool calls.
		if count <= 0 {
			count = len(s.session.LastInteractionToolCalls)
		}
		if count <= 0 {
			return fmt.Errorf("no steps to refactor")
		}
		if len(s.session.CurrentScript.Steps) < count {
			return fmt.Errorf("not enough steps to refactor")
		}
		startIdx := len(s.session.CurrentScript.Steps) - count
		stepsToGroup = s.session.CurrentScript.Steps[startIdx:]
		s.session.CurrentScript.Steps = s.session.CurrentScript.Steps[:startIdx]
	}

	if len(stepsToGroup) == 0 {
		return fmt.Errorf("no steps to refactor")
	}

	if mode == "script" {
		if name == "" {
			return fmt.Errorf("script name required")
		}
		// Create new script
		newScript := ai.Script{
			Name:            name,
			Steps:           stepsToGroup,
			TransactionMode: "single", // Default to single tx for extracted scripts
		}
		// Save it
		if err := s.saveScript(context.Background(), newScript); err != nil {
			return err
		}
		// Add script step
		s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, ai.ScriptStep{
			Type:       "call_script",
			ScriptName: name,
		})
	} else if mode == "block" {
		// Add block step
		s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, ai.ScriptStep{
			Type:  "block",
			Steps: stepsToGroup,
		})
	}

	return nil
}

func (s *Service) saveScript(ctx context.Context, script ai.Script) error {
	scriptDB := s.getScriptDB()
	if scriptDB == nil {
		return fmt.Errorf("script database not available")
	}
	tx, err := scriptDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	store, err := scriptDB.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	if err := store.Save(ctx, "general", script.Name, &script); err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("failed to save script: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *Service) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	// Ensure statelessness for non-playback sessions (Interactive & Recording)
	defer func() {
		if s.GetSessionMode() != SessionModePlayback && s.session.Transaction != nil {
			// Rollback if not committed (safety)
			// If it was committed, it should have been cleared.
			// If it's still here, it's a leak.
			s.session.Transaction.Rollback(ctx)
			s.session.Transaction = nil
			s.session.Variables = nil
		}
	}()

	// Clear buffer at start of Ask
	s.session.LastInteractionToolCalls = []ai.ScriptStep{}

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
			if db == nil && p.CurrentDB != "" {
				if opts, ok := s.databases[p.CurrentDB]; ok {
					db = database.NewDatabase(opts)
				}
			}
		}
	} else if p := ai.GetSessionPayload(ctx); p != nil {
		// Payload already exists in context, respect it.
		// Ensure db is set if needed.
		if db == nil && p.CurrentDB != "" {
			if opts, ok := s.databases[p.CurrentDB]; ok {
				db = database.NewDatabase(opts)
			}
		}
	} else {
		// If no payload provided, create a default one from session state
		p := &ai.SessionPayload{
			CurrentDB: s.session.CurrentDB,
		}
		if s.session.Transaction != nil {
			p.Transaction = s.session.Transaction
			p.Variables = s.session.Variables
			p.ExplicitTransaction = true
		}
		// If Transactions map is needed, we might need to store it in session too?
		// Currently RunnerSession doesn't seem to have Transactions map.
		// But for single-DB transaction it works.
		ctx = context.WithValue(ctx, "session_payload", p)

		// Set db if available
		if db == nil && p.CurrentDB != "" {
			if opts, ok := s.databases[p.CurrentDB]; ok {
				db = database.NewDatabase(opts)
			}
		}
	}

	// Inject ScriptRecorder into context
	ctx = context.WithValue(ctx, ai.CtxKeyScriptRecorder, s)

	// Capture "ask" step for potential manual addition
	// We do this BEFORE handling /record or /play so those commands themselves aren't captured as "ask" steps
	// We explicitly exclude "last-tool" and any slash commands from being recorded as user intent.
	if !strings.HasPrefix(query, "/") && query != "last-tool" {
		// Only record "ask" step if NOT in compiled mode
		// If in compiled mode, we wait for the tool execution to record the command step
		if !s.session.Recording || s.session.RecordingMode != "compiled" {
			s.RecordStep(ctx, ai.ScriptStep{
				Type:   "ask",
				Prompt: query,
			})
		}
	}

	// Handle Session Commands (Scripts, Recording, etc.)
	if resp, handled, err := s.handleSessionCommand(ctx, query, db); handled {
		return resp, err
	}

	// If we are recording, we do NOT want to execute the query against the LLM if it's a transaction command
	// that was handled by the tool but skipped execution.
	// However, the tool execution happens inside the LLM loop (or via direct tool call if we supported that).
	// Since we are using an LLM, we must let it run.
	// But wait, if the user says "begin transaction", the LLM will call the tool.
	// The tool will see the recorder and return "Recorded...".
	// The LLM will then see that output and likely say "I have recorded the transaction start".
	// This is fine.

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
		return resp, err
	}

	// 1. Search for context
	hits, err := s.Search(ctx, query, 10)
	if err != nil {
		return "", fmt.Errorf("retrieval failed: %w", err)
	}

	// 2. Construct Prompt
	contextText := s.formatContext(hits)
	var systemPrompt string
	if s.domain != nil {
		systemPrompt, _ = s.domain.Prompt(ctx, "system")
	}

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
				log.Warn("Failed to initialize requested provider, falling back to default", "provider", provider, "error", err)
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
			if s.session.Recording {
				s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, ai.ScriptStep{
					Type:   "ask",
					Prompt: string(b), // Store raw tool call as prompt
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
				// De-obfuscate Tool Arguments before returning to caller.

				// 1. Parse JSON FIRST to get the exact values the LLM returned
				var toolCall struct {
					Tool string         `json:"tool"`
					Args map[string]any `json:"args"`
				}

				// We try to unmarshal the text directly.
				// If the LLM returned valid JSON (even with obfuscated values), this will succeed.
				if err := json.Unmarshal([]byte(text), &toolCall); err == nil {
					// 2. Sanitize Args
					var sanitize func(any) any
					sanitize = func(v any) any {
						switch val := v.(type) {
						case string:
							// a. Remove Markdown bold/italics/code wrappers
							val = strings.Trim(val, "*_`")
							// b. Replace NBSP with space and Trim whitespace
							val = strings.ReplaceAll(val, "\u00a0", " ")
							val = strings.TrimSpace(val)
							// c. De-obfuscate if enabled
							if s.EnableObfuscation {
								val = obfuscation.GlobalObfuscator.DeobfuscateText(val)
							}
							return val
						case []any:
							for i, item := range val {
								val[i] = sanitize(item)
							}
							return val
						case map[string]any:
							for k, item := range val {
								val[k] = sanitize(item)
							}
							return val
						default:
							return val
						}
					}

					for k, v := range toolCall.Args {
						toolCall.Args[k] = sanitize(v)
					}

					// Inject Database from Options if missing
					if db != nil {
						// Inject the database instance into args for the ToolExecutor.
						toolCall.Args["_db_instance"] = db
					}

					// Auto-Transaction Management for Tool Execution
					// We ensure that if a database is present, we wrap the tool execution in a transaction.
					// This prevents leaving open transactions if the tool doesn't manage them,
					// and ensures atomic execution of the tool's operations.
					var tx sop.Transaction
					if db != nil {
						if p := ai.GetSessionPayload(ctx); p != nil && p.Transaction == nil {
							var err error
							tx, err = db.BeginTransaction(ctx, sop.ForWriting)
							if err != nil {
								return "", fmt.Errorf("failed to begin auto-transaction: %w", err)
							}
							p.Transaction = tx
						}
					}

					// Execute Tool
					result, err := executor.Execute(ctx, toolCall.Tool, toolCall.Args)

					// Commit or Rollback Auto-Transaction
					if tx != nil {
						if err != nil {
							// If tool failed, rollback
							tx.Rollback(ctx)
						} else {
							// If tool succeeded, commit
							if commitErr := tx.Commit(ctx); commitErr != nil {
								return "", fmt.Errorf("tool execution succeeded but transaction commit failed: %w", commitErr)
							}
						}
						// Clear from payload to avoid reuse if p is reused
						if p := ai.GetSessionPayload(ctx); p != nil {
							p.Transaction = nil
						}
						// Also clear session transaction to ensure statelessness
						s.session.Transaction = nil
					} else if !s.session.Recording && s.session.Transaction != nil {
						// Ensure statelessness for non-script sessions even if no auto-transaction was started
						if err != nil {
							s.session.Transaction.Rollback(ctx)
						} else {
							// We commit if the tool execution was successful
							s.session.Transaction.Commit(ctx)
						}
						s.session.Transaction = nil
						s.session.Variables = nil
					}

					if err != nil {
						return "", fmt.Errorf("tool execution failed: %w", err)
					}

					// Always capture as last step (for manual addition)
					s.session.LastStep = &ai.ScriptStep{
						Type:    "command",
						Command: toolCall.Tool,
						Args:    toolCall.Args,
					}

					// Record Tool Call if recording
					if s.session.Recording {
						if s.session.RecordingMode == "compiled" {
							if err := s.appendStepToCurrentScript(ctx, *s.session.LastStep); err != nil {
								log.Error("failed to append step to script", "error", err)
							}
						}
						// In natural mode, we already recorded the prompt via s.RecordStep at the top.
						// So we do NOT record the tool call here to avoid double execution.
					}

					return result, nil
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
	if s.session.Recording && !toolRecorded {
		// Only record "say" step if NOT in compiled mode
		// In compiled mode, we only care about the commands (tools)
		if s.session.RecordingMode != "compiled" {
			s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, ai.ScriptStep{
				Type:    "say",
				Message: finalText,
			})
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
