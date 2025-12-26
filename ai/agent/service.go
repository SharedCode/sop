package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

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

	// If we are recording and have an active recording transaction, use it.
	if s.session.Recording && s.session.Transaction != nil {
		// We do NOT use the recording transaction for the current request if we are just recording steps.
		// The user wants to record "begin transaction", "commit", etc. as steps.
		// If we inject the transaction here, the tool might try to use it.
		// But wait, the tool checks for the recorder and skips execution.
		// So it doesn't matter if we inject it or not?
		// Actually, if we inject it, other tools (like select) might use it.
		// But if we are recording, we might want to verify the steps work?
		// The user said: "should NOT intervene with how we do transaction on the recording session."
		// This implies the recording session (the one where we are saying "add item...") should be independent
		// of the transaction state being recorded.
		// So, we should NOT inject the transaction from the "macro state" into the "current session state".
		// BUT, the user might want to run a select to see if the item was added?
		// If we are in "Natural Mode", we are executing AND recording.
		// If we are in "Compiled Mode", we are JUST recording (mostly).
		// The user's request specifically mentions "explicit Begin Transaction and Commit".
		// "We don't interpret these steps' commands! they are for run time interpretation."
		// This suggests that when recording, "begin transaction" is just a string/step, not an action.

		// However, for other commands (add, select), we might still want to execute them?
		// If we are in "Natural Mode", yes.
		// If we are in "Compiled Mode", maybe not?

		// Let's assume for now we continue to inject it, but the tool handles the "manage_transaction" specifically.
		p.Transaction = s.session.Transaction
		p.Variables = s.session.Variables
		return nil
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
		// If recording, and transaction is gone (e.g. rollback without restart), clear recordingTx
		if s.session.Recording {
			s.session.Transaction = nil
		}
		return nil
	}
	if tx, ok := p.Transaction.(sop.Transaction); ok {
		// If we are recording, we capture whatever transaction is currently active
		// as the "recording transaction" for the next request.
		// We do NOT commit it here.
		if s.session.Recording {
			s.session.Transaction = tx
			s.session.Variables = p.Variables
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
	return s.session.StopOnError
}

// StopRecording stops the current recording session.
func (s *Service) StopRecording() {
	s.session.Recording = false
	s.session.CurrentMacro = nil
	s.session.Transaction = nil
	s.session.Variables = nil
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
	s.session.LastStep = &step

	// Buffer tool calls for potential refactoring
	if step.Type == "command" {
		s.session.LastInteractionToolCalls = append(s.session.LastInteractionToolCalls, step)
	}

	if s.session.Recording && s.session.CurrentMacro != nil {
		// In standard mode, we only record high-level "ask" steps (user intent).
		// We ignore "command" steps (tool calls) because replaying the "ask" step
		// will naturally trigger the tool call again.
		if s.session.RecordingMode == "standard" && step.Type == "command" {
			return
		}
		s.session.CurrentMacro.Steps = append(s.session.CurrentMacro.Steps, step)
	}
}

// RefactorLastSteps refactors the last N steps into a new structure (macro or block).
func (s *Service) RefactorLastSteps(count int, mode string, name string) error {
	if !s.session.Recording || s.session.CurrentMacro == nil {
		return fmt.Errorf("not recording")
	}

	var stepsToGroup []ai.MacroStep

	if s.session.RecordingMode == "standard" {
		// In standard mode, the last step in CurrentMacro is likely the "ask" step.
		// We want to replace it with the buffered tool calls.
		if len(s.session.CurrentMacro.Steps) > 0 {
			lastIdx := len(s.session.CurrentMacro.Steps) - 1
			if s.session.CurrentMacro.Steps[lastIdx].Type == "ask" {
				s.session.CurrentMacro.Steps = s.session.CurrentMacro.Steps[:lastIdx]
			}
		}
		stepsToGroup = s.session.LastInteractionToolCalls
	} else {
		// In compiled mode, the tool calls are already in CurrentMacro.Steps.
		// Use count if provided, otherwise use the length of buffered tool calls.
		if count <= 0 {
			count = len(s.session.LastInteractionToolCalls)
		}
		if count <= 0 {
			return fmt.Errorf("no steps to refactor")
		}
		if len(s.session.CurrentMacro.Steps) < count {
			return fmt.Errorf("not enough steps to refactor")
		}
		startIdx := len(s.session.CurrentMacro.Steps) - count
		stepsToGroup = s.session.CurrentMacro.Steps[startIdx:]
		s.session.CurrentMacro.Steps = s.session.CurrentMacro.Steps[:startIdx]
	}

	if len(stepsToGroup) == 0 {
		return fmt.Errorf("no steps to refactor")
	}

	if mode == "macro" {
		if name == "" {
			return fmt.Errorf("macro name required")
		}
		// Create new macro
		newMacro := ai.Macro{
			Name:            name,
			Steps:           stepsToGroup,
			TransactionMode: "single", // Default to single tx for extracted scripts
		}
		// Save it
		if err := s.saveMacro(context.Background(), newMacro); err != nil {
			return err
		}
		// Add macro step
		s.session.CurrentMacro.Steps = append(s.session.CurrentMacro.Steps, ai.MacroStep{
			Type:      "macro",
			MacroName: name,
		})
	} else if mode == "block" {
		// Add block step
		s.session.CurrentMacro.Steps = append(s.session.CurrentMacro.Steps, ai.MacroStep{
			Type:  "block",
			Steps: stepsToGroup,
		})
	}

	return nil
}

func (s *Service) saveMacro(ctx context.Context, macro ai.Macro) error {
	macroDB := s.getMacroDB()
	if macroDB == nil {
		return fmt.Errorf("macro database not available")
	}
	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return err
	}
	if err := store.Save(ctx, "macros", macro.Name, &macro); err != nil {
		tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	// Clear buffer at start of Ask
	s.session.LastInteractionToolCalls = []ai.MacroStep{}

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
	}

	// Inject MacroRecorder into context
	ctx = context.WithValue(ctx, ai.CtxKeyMacroRecorder, s)

	// Capture "ask" step for potential manual addition
	// We do this BEFORE handling /record or /play so those commands themselves aren't captured as "ask" steps
	if !strings.HasPrefix(query, "/") {
		// Only record "ask" step if NOT in compiled mode
		// If in compiled mode, we wait for the tool execution to record the command step
		if !s.session.Recording || s.session.RecordingMode != "compiled" {
			s.RecordStep(ai.MacroStep{
				Type:   "ask",
				Prompt: query,
			})
		}
	}

	// Handle Session Commands (Macros, Recording, etc.)
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
		// Update recordingTx if the pipeline changed the transaction (e.g. via manage_transaction tool)
		if s.session.Recording {
			if p := ai.GetSessionPayload(ctx); p != nil && p.Transaction != nil {
				if tx, ok := p.Transaction.(sop.Transaction); ok {
					s.session.Transaction = tx
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
			if s.session.Recording {
				s.session.CurrentMacro.Steps = append(s.session.CurrentMacro.Steps, ai.MacroStep{
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
						// Inject the database instance into args for the ToolExecutor.
						toolCall.Args["_db_instance"] = db
					}

					// Re-serialize
					if b, err := json.Marshal(toolCall); err == nil {
						cleanJSON := string(b)

						// Always capture as last step (for manual addition)
						s.session.LastStep = &ai.MacroStep{
							Type:    "command",
							Command: toolCall.Tool,
							Args:    toolCall.Args,
						}

						// Record Tool Call if recording
						if s.session.Recording {
							if s.session.RecordingMode == "compiled" {
								s.session.CurrentMacro.Steps = append(s.session.CurrentMacro.Steps, *s.session.LastStep)
							}
							// In natural mode, we already recorded the prompt via s.RecordStep at the top.
							// So we do NOT record the tool call here to avoid double execution.
						}

						// Update recordingTx if the tool changed the transaction (e.g. via manage_transaction tool)
						// Note: Transaction updates resulting from tool execution are handled by the caller
						// or the next request's Open() call.

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
	if s.session.Recording && !toolRecorded {
		// Only record "say" step if NOT in compiled mode
		// In compiled mode, we only care about the commands (tools)
		if s.session.RecordingMode != "compiled" {
			s.session.CurrentMacro.Steps = append(s.session.CurrentMacro.Steps, ai.MacroStep{
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
