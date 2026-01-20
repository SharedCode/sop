package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/obfuscation"
	sopdb "github.com/sharedcode/sop/database"
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
	// Feature Flags
	EnableHistoryInjection bool

	// Session State
	session *RunnerSession
}

// Check that Service implements ScriptRecorder
var _ ai.ScriptRecorder = (*Service)(nil)

// NewService creates a new agent service for a specific domain.
func NewService(domain ai.Domain[map[string]any], systemDB *database.Database, databases map[string]sop.DatabaseOptions, generator ai.Generator, pipeline []PipelineStep, registry map[string]ai.Agent[map[string]any], enableObfuscation bool) *Service {
	return &Service{
		domain:                 domain,
		systemDB:               systemDB,
		databases:              databases,
		generator:              generator,
		pipeline:               pipeline,
		registry:               registry,
		EnableObfuscation:      enableObfuscation,
		EnableHistoryInjection: false, // Default to OFF for simple machine mode (can be enabled via config)
		session:                NewRunnerSession(),
	}
}

// SetFeature allows toggling of agent features at runtime.
func (s *Service) SetFeature(feature string, enabled bool) {
	switch feature {
	case "history_injection":
		s.EnableHistoryInjection = enabled
	case "obfuscation":
		s.EnableObfuscation = enabled
	}
}

// TopicAssessment is the structure returned by the generic router.
type TopicAssessment struct {
	IsNewTopic    bool   `json:"is_new_topic"`
	TopicUUID     string `json:"topic_uuid,omitempty"` // If not new, the UUID of the existing graph
	NewTopicLabel string `json:"new_topic_label,omitempty"`
	Reasoning     string `json:"reasoning"`
}

// identifyTopic determines if the query belongs to an existing conversation graph or starts a new one.
func (s *Service) identifyTopic(ctx context.Context, query string) (*TopicAssessment, error) {
	if s.session.Memory == nil || len(s.session.Memory.Threads) == 0 {
		return &TopicAssessment{IsNewTopic: true, Reasoning: "No history exists."}, nil
	}

	// Prepare list of recent topics
	var summaries []string
	for i := len(s.session.Memory.Order) - 1; i >= 0; i-- {
		id := s.session.Memory.Order[i]
		thread := s.session.Memory.Threads[id]
		// Get last interaction
		lastMsg := ""
		if len(thread.Exchanges) > 0 {
			lastMsg = thread.Exchanges[len(thread.Exchanges)-1].Content
			if len(lastMsg) > 50 {
				lastMsg = lastMsg[:50] + "..."
			}
		}
		statusSuffix := ""
		if thread.Status == "concluded" {
			statusSuffix = " [CONCLUDED]"
		}
		summaries = append(summaries, fmt.Sprintf("- ID: %s | Label: %s%s | Last Msg: %s", thread.ID, thread.Label, statusSuffix, lastMsg))
	}
	topicsBlock := strings.Join(summaries, "\n")

	prompt := fmt.Sprintf(`You are a conversation manager. Analyze the User Query and decide if it is a follow-up to an existing topic or a new topic.
Existing Topics (Most Recent First):
%s

User Query: "%s"

Instructions:
1. If the query strictly refers to the context of a previous topic (e.g. "change it to blue", "what about the other one?"), select that Topic ID.
2. If the query starts a completely new subject, mark as New Topic.
3. Provide a JSON response.

Format:
{
  "is_new_topic": true/false,
  "topic_uuid": "UUID-STRING",
  "new_topic_label": "Short Label if new",
  "reasoning": "Short explanation"
}
`, topicsBlock, query)

	// Combine instructions into the prompt since GenOptions doesn't support SystemPrompt
	fullPrompt := "Answer in strict JSON.\n" + prompt

	output, err := s.generator.Generate(ctx, fullPrompt, ai.GenOptions{
		Temperature: 0.1, // Deterministic
	})
	if err != nil {
		return nil, err
	}

	// Sanitize JSON
	jsonStr := strings.TrimSpace(output.Text)
	jsonStr = strings.TrimPrefix(jsonStr, "```json")
	jsonStr = strings.TrimPrefix(jsonStr, "```")
	jsonStr = strings.TrimSuffix(jsonStr, "```")

	var assessment TopicAssessment
	if err := json.Unmarshal([]byte(jsonStr), &assessment); err != nil {
		// Fallback if JSON fails
		log.Warn("Failed to parse topic assessment JSON", "error", err, "response", output.Text)
		return &TopicAssessment{IsNewTopic: true, Reasoning: "JSON parse failure"}, nil
	}

	return &assessment, nil
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
	if s.session.Transaction != nil {
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

	// Check if configured System DB matches
	var dbToOpen *database.Database
	if (p.CurrentDB == "system" || p.CurrentDB == "SystemDB") && s.systemDB != nil {
		dbToOpen = s.systemDB
	} else if dbOpts, ok := s.databases[p.CurrentDB]; ok {
		dbToOpen = database.NewDatabase(dbOpts)
	}

	if dbToOpen != nil {
		// Start transaction
		tx, err := dbToOpen.BeginTransaction(ctx, sop.ForWriting)
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
	// We no longer clear the Memory here to support cross-request short-term memory (Conversation Graphs).
	// The LRU limit (20 items) prevents unbounded growth.
	if s.session != nil {
		s.session.Variables = nil
		s.session.CurrentScript = nil
		// s.session.LastStep = nil // Preserved for /last-tool
		// s.session.LastInteractionToolCalls = nil // Preserved for /last-tool
		s.session.PendingRefinement = nil
	}

	p := ai.GetSessionPayload(ctx)
	if p == nil || p.Transaction == nil {
		return nil
	}
	if tx, ok := p.Transaction.(sop.Transaction); ok {
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
	return nil
}

// Domain returns the underlying domain of the service.
func (s *Service) Domain() ai.Domain[map[string]any] {
	return s.domain
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
		// Try to unmarshal if it's a string to log it as a JSON object instead of a string
		if scriptStr, ok := script.(string); ok {
			var scriptJSON interface{}
			if err := json.Unmarshal([]byte(scriptStr), &scriptJSON); err == nil {
				log.Debug("Service.GetLastToolInstructions: Retrieving script", "script", scriptJSON)
			} else {
				log.Debug("Service.GetLastToolInstructions: Retrieving script", "script", script)
			}
		} else {
			log.Debug("Service.GetLastToolInstructions: Retrieving script", "script", script)
		}
	} else {
		keys := make([]string, 0, len(targetStep.Args))
		for k := range targetStep.Args {
			keys = append(keys, k)
		}
		log.Debug(fmt.Sprintf("Service.GetLastToolInstructions: Retrieving command '%s' without script. Args keys: %v", targetStep.Command, keys))
	}

	// Reconstruct the tool call structure with unmarshaled script if present
	args := make(map[string]any)
	for k, v := range targetStep.Args {
		args[k] = v
	}

	if script, ok := args["script"]; ok {
		if scriptStr, ok := script.(string); ok {
			var scriptJSON interface{}
			if err := json.Unmarshal([]byte(scriptStr), &scriptJSON); err == nil {
				args["script"] = scriptJSON
			}
		}
	}

	toolCall := map[string]any{
		"tool": targetStep.Command,
		"args": args,
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
			log.Debug(fmt.Sprintf("Service.RecordStep: Drafting script. Type: %T, Value: %+v", script, script))
		} else {
			keys := make([]string, 0, len(step.Args))
			for k := range step.Args {
				keys = append(keys, k)
			}
			log.Debug(fmt.Sprintf("Service.RecordStep: Drafting command '%s' without script. Args keys: %v", step.Command, keys))
		}
	}

	// Deep copy args to ensure we persist the exact state at this moment
	// and protect against future mutations of the map by the caller.
	if step.Args != nil {
		step.Args = deepCopyMap(step.Args)
	}

	// Always capture the last step for potential manual addition
	s.session.LastStep = &step

	// If we are actively drafting a script, append the step
	if s.session.CurrentScript != nil {
		s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, step)
		log.Debug("Service.RecordStep: Appended step to CurrentScript", "script_name", s.session.CurrentScript.Name, "step_count", len(s.session.CurrentScript.Steps))
	}

	// Buffer tool calls for potential refactoring
	if step.Type == "command" {
		s.session.LastInteractionToolCalls = append(s.session.LastInteractionToolCalls, step)
	}
}

// RefactorLastSteps implements the ScriptRecorder interface
func (s *Service) RefactorLastSteps(count int, mode string, name string) error {
	// TODO: Implement script refactoring logic
	return fmt.Errorf("not implemented")
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

func (s *Service) getLLMKnowledge(ctx context.Context) string {
	if s.systemDB == nil {
		return ""
	}
	// Use a short-lived read transaction
	tx, err := s.systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return ""
	}
	defer tx.Rollback(ctx)

	var sb strings.Builder

	// Open Knowledge Store
	ks, err := OpenKnowledgeStore(ctx, tx, s.systemDB.Options())
	if err != nil {
		return ""
	}

	// SCALABILITY FIX: Instead of loading everything, we only load "Core" namespaces.
	// We load 'memory' (General instructions) and 'term' (Business glossary).
	// Other namespaces (like 'schema' or domain-specifics) must be requested by the agent via tools.

	namespacesToLoad := []string{"memory", "term"}

	// SCALABILITY FIX 2: Load Semantic Memory namespaces
	semanticNamespaces := []string{"vocabulary", "rule", "correction"}

	// Add current DB domain if applicable
	if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
		// e.g. "finance" for "finance_db"
		// Simple heuristic: usage of db name as namespace
		namespacesToLoad = append(namespacesToLoad, p.CurrentDB)
	}

	sb.WriteString("\n[Global Instructions/Knowledge]\n")

	for _, ns := range namespacesToLoad {
		// ListContent is optimized to only scan the namespace
		items, _ := ks.ListContent(ctx, ns)
		if len(items) > 0 {
			for k, v := range items {
				// Format: "term:EBITDA: ..."
				sb.WriteString(fmt.Sprintf("%s:%s:\n%s\n\n", ns, k, v))
			}
		}
	}

	// [Semantic Memory Injection]
	// We explicitly format these for the LLM to use as rules, rather than raw dumps.
	didHeader := false
	for _, ns := range semanticNamespaces {
		items, _ := ks.ListContent(ctx, ns)
		if len(items) > 0 {
			if !didHeader {
				sb.WriteString("\n[Semantic Memory & Rules]\n(You MUST apply these mappings and rules when interpreting queries)\n")
				didHeader = true
			}
			for k, v := range items {
				// Try to parse JSON for cleaner display, otherwise fallback to raw
				var parsed map[string]any
				if err := json.Unmarshal([]byte(v), &parsed); err == nil {
					// Format based on namespace
					if ns == "vocabulary" {
						target, _ := parsed["target"].(string)
						desc, _ := parsed["description"].(string)
						sb.WriteString(fmt.Sprintf("- Term '%s' -> Maps to field: '%s' (%s)\n", k, target, desc))
					} else if ns == "rule" {
						cond, _ := parsed["condition"].(string)
						desc, _ := parsed["description"].(string)
						sb.WriteString(fmt.Sprintf("- Rule '%s': Apply condition \"%s\" (%s)\n", k, cond, desc))
					} else if ns == "correction" {
						instr, _ := parsed["instruction"].(string)
						sb.WriteString(fmt.Sprintf("- Correction '%s': %s\n", k, instr))
					} else {
						sb.WriteString(fmt.Sprintf("- %s:%s: %s\n", ns, k, v))
					}
				} else {
					// Fallback for non-JSON strings
					sb.WriteString(fmt.Sprintf("- %s:%s: %s\n", ns, k, v))
				}
			}
		}
	}

	// Add tip about how to get more
	sb.WriteString("Note: Extended knowledge (schemas, other domains) is available via 'manage_knowledge' tool (action='read' or search).\n")

	return sb.String()
}

func (s *Service) getDomainKnowledge(ctx context.Context, dbName string) string {
	if dbName == "" || dbName == "system" || dbName == "SystemDB" {
		return ""
	}
	opts, ok := s.databases[dbName]
	if !ok {
		return ""
	}
	// Temporarily open DB? Or assuming it's accessible.
	// database.NewDatabase(opts) creates a handler.
	db := database.NewDatabase(opts)

	// Use a short-lived read transaction
	tx, err := db.BeginTransaction(ctx, sop.NoCheck)
	if err != nil {
		return ""
	}
	defer tx.Rollback(ctx)

	store, err := sopdb.OpenBtree[string, string](ctx, db.Options(), "domain_knowledge", tx, nil)
	if err != nil {
		return ""
	}

	var sb strings.Builder
	if ok, err := store.First(ctx); ok && err == nil {
		sb.WriteString(fmt.Sprintf("\n[Domain Knowledge (%s)]\n", dbName))
		for {
			item := store.GetCurrentKey()
			k := item.Key
			v, _ := store.GetCurrentValue(ctx)
			sb.WriteString(fmt.Sprintf("%s:\n%s\n\n", k, v))

			if ok, _ := store.Next(ctx); !ok {
				break
			}
		}
	}
	return sb.String()
}

func (s *Service) getToolInfo(ctx context.Context, toolName string) (string, error) {
	if s.systemDB == nil {
		return "", fmt.Errorf("system DB not available")
	}
	tx, err := s.systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	store, err := sopdb.OpenBtree[KnowledgeKey, string](ctx, s.systemDB.Options(), "llm_knowledge", tx, nil)
	if err != nil {
		return "", err
	}

	if found, err := store.Find(ctx, KnowledgeKey{Category: "tool", Name: toolName}, false); found && err == nil {
		val, err := store.GetCurrentValue(ctx)
		if err != nil {
			return "", err
		}
		return val, nil
	}
	return "", fmt.Errorf("tool info for '%s' not found", toolName)
}

// registerTools sets up the tools available to the LLM.
func (s *Service) registerTools() {
	if s.registry == nil {
		return
	}
	// We inject `conclude_topic` by directly defining it in the interface.
	// Since s.registry is map[string]ai.Agent (where ai.Agent is an interface),
	// this approach (editing registry from service) assumes Service *owns* the orchestration.
	// However, `ai.Agent` interface expects `Execute` method.
	// We need a wrapper.

	// Create a wrapper agent for ad-hoc service tools
	s.registry["conclude_topic"] = &AdHocAgent{
		Name: "conclude_topic",
		Handler: func(ctx context.Context, args map[string]interface{}) (string, error) {
			return s.handleConcludeTopic(ctx, args)
		},
	}
}

// AdHocAgent implements ai.Agent for simple function wrappers
type AdHocAgent struct {
	Name    string
	Handler func(ctx context.Context, args map[string]interface{}) (string, error)
}

// Implement ai.Agent[map[string]any] interface
func (a *AdHocAgent) Open(ctx context.Context) error  { return nil }
func (a *AdHocAgent) Close(ctx context.Context) error { return nil }
func (a *AdHocAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return nil, nil
}
func (a *AdHocAgent) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	return "", nil
}

// Implement ToolProvider interface
func (a *AdHocAgent) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	if toolName == a.Name {
		return a.Handler(ctx, args)
	}
	return "", fmt.Errorf("unknown tool: %s", toolName)
}

// handleConcludeTopic implements the session-aware topic conclusion.
func (s *Service) handleConcludeTopic(ctx context.Context, args map[string]interface{}) (string, error) {
	summary, _ := args["summary"].(string)
	label, _ := args["topic_label"].(string)

	if summary == "" {
		return "", fmt.Errorf("summary is required")
	}

	if s.session.Memory == nil || len(s.session.Memory.CurrentThreadID) == 0 {
		return "No active topic to conclude.", nil
	}

	thread := s.session.Memory.GetCurrentThread()
	if thread == nil {
		return "Current topic not found.", nil
	}

	thread.Conclusion = summary
	thread.Status = "concluded"
	if label != "" {
		thread.Label = label
	}

	return fmt.Sprintf("Topic '%s' concluded. Summary saved: %s", thread.Label, summary), nil
}

func (s *Service) Ask(ctx context.Context, query string, opts ...ai.Option) (string, error) {
	// Ensure statelessness for non-playback sessions (Interactive & Drafting)
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
	var forcedDBName string

	if val, ok := cfg.Values["database"]; ok {
		if d, ok := val.(*database.Database); ok {
			db = d
		} else if dName, ok := val.(string); ok && dName != "" {
			// If a string name is provided, use it to resolve DB and set as forcedDBName
			forcedDBName = dName
			if opts, ok := s.databases[dName]; ok {
				db = database.NewDatabase(opts)
			}
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
		// Use forcedDBName if provided via Ask options, otherwise session state

		// Ensure we have a tool executor
		// If the caller didn't provide one, we use the ServiceToolExecutor which delegates to registered agents.
		if ctx.Value(ai.CtxKeyExecutor) == nil {
			executor := &ServiceToolExecutor{s: s}
			ctx = context.WithValue(ctx, ai.CtxKeyExecutor, executor)
		}

		// 1. Prepare Context (History, DB schema, etc.)
		targetDB := s.session.CurrentDB
		if forcedDBName != "" {
			targetDB = forcedDBName
		}

		p := &ai.SessionPayload{
			CurrentDB: targetDB,
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
	// We do this BEFORE handling /create or /play so those commands themselves aren't captured as "ask" steps
	// We explicitly exclude "last-tool" and any slash commands from being recorded as user intent.
	if !strings.HasPrefix(query, "/") && query != "last-tool" {
		s.RecordStep(ctx, ai.ScriptStep{
			Type:   "ask",
			Prompt: query,
		})
	}

	// Handle Session Commands (Scripts, Drafting, etc.)
	if resp, handled, err := s.handleSessionCommand(ctx, query, db); handled {
		return resp, err
	}

	// If we are drafting, we do NOT want to execute the query against the LLM if it's a transaction command
	// that was handled by the tool but skipped execution.
	// However, the tool execution happens inside the LLM loop (or via direct tool call if we supported that).
	// Since we are using an LLM, we must let it run.
	// But wait, if the user says "begin transaction", the LLM will call the tool.
	// The tool will see the recorder and return "recorded...".
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

	// Ensure tools are registered (session specific registration)
	s.registerTools()

	// 0.5 Topic Identification / Routing (STM)
	var topicAssessment *TopicAssessment
	// Only run identification if we have history AND history injection is enabled.
	// If injection is disabled, we must not send history summaries to the LLM (side-channel leak).
	if s.EnableHistoryInjection && s.session.Memory != nil {
		// Only run identification if we have history
		assessment, err := s.identifyTopic(ctx, query)
		if err != nil {
			log.Warn("Topic identification failed, defaulting to new topic", "error", err)
			topicAssessment = &TopicAssessment{IsNewTopic: true}
		} else {
			topicAssessment = assessment
		}

		// Apply STM Logic: Promote existing thread if identified
		if !topicAssessment.IsNewTopic && topicAssessment.TopicUUID != "" {
			topicID, err := sop.ParseUUID(topicAssessment.TopicUUID)
			if err == nil {
				s.session.Memory.PromoteThread(topicID)
			}
		}
	} else {
		// Initialize memory if missing (defensive)
		s.session.Memory = NewShortTermMemory()
		topicAssessment = &TopicAssessment{IsNewTopic: true}
	}

	// 1. Search for context
	// We must ensure that s.Search(term) does not return "User1, User2" from previous run.
	// Since Search uses s.domain, and we inject a mock domain or real one:
	// If the domain is just a vector store, it returns unrelated text.
	// But if the "history" or "recent output" is being indexed...

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

	// 2a. Inject LLM Knowledge (Lifecycle "On Init" simulation)
	if known := s.getLLMKnowledge(ctx); known != "" {
		systemPrompt = fmt.Sprintf("%s\n\n%s", systemPrompt, known)
	}

	// 2b. Inject Domain Knowledge (Lifecycle "On Init" simulation)
	currentDBName := ""
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDBName = p.CurrentDB
	} else {
		currentDBName = s.session.CurrentDB
	}
	if forcedDBName != "" {
		currentDBName = forcedDBName
	}
	if domainKnown := s.getDomainKnowledge(ctx, currentDBName); domainKnown != "" {
		systemPrompt = fmt.Sprintf("%s\n\n%s", systemPrompt, domainKnown)
	}

	// 2c. Tool Usage Hint for Knowledge Retrieval
	systemPrompt = fmt.Sprintf("%s\n\n[Tool Usage Note]\nYou can use the tool \"gettoolinfo\" with argument \"tool\" (e.g., \"gettoolinfo('execute_script')\") to get detailed usage instructions for any tool if you are unsure about its parameters or behavior.", systemPrompt)

	// [Managed Conversation Protocol]
	conversationProtocol := `
[Conversation Management]
You are capable of managing the conversation flow.
- A "Conversation Thread" tracks the current topic.
- When an issue is resolved, or a significant sub-task is completed, and you are ready to switch context, use the 'conclude_topic(summary, topic_label)' tool.
- This helps cleaner context management.
`
	systemPrompt = fmt.Sprintf("%s\n%s", systemPrompt, conversationProtocol)

	// [Active Learning Protocol]
	learningProtocol := `
[Active Learning & Refinement Protocol]
You are a self-correcting agent. If the user corrects your output, provides a definition, or establishes a preference (e.g., "Use 'Client' instead of 'User'", "Always check X before Y"):
1. ACKNOWLEDGE the correction.
2. IMMEDIATELY use the 'manage_knowledge' tool to save this rule for future consistency.
   - For synonyms/renaming: Use namespace="vocabulary", key="term_name", value={"target": "preferred_term", "type": "synonym"}.
   - For logic/process rules: Use namespace="rule", key="rule_name", value={"condition": "context description", "instruction": "the rule content"}.
   - For fixing mistakes: Use namespace="correction", key="error_name", value={"error": "what you did wrong", "fix": "what to do instead"}.
3. CONFIRM to the user that this has been memorized.
   Example: "I have updated my knowledge base: 'Cost' will now be interpreted as 'TotalAmount'."
`
	systemPrompt = fmt.Sprintf("%s\n%s", systemPrompt, learningProtocol)

	// [Regression Fix]
	// If EnableHistoryInjection is false, we should also ensure that formatContext
	// didn't accidentally include history-like artifacts if they were in the "hits".
	// But more importantly, check if we accidentally appended historyText anyway.
	var historyText string
	if s.EnableHistoryInjection && s.session.Memory != nil && len(s.session.Memory.Order) > 0 {
		var historyBuilder strings.Builder
		// Iterate through threads in order
		for _, threadID := range s.session.Memory.Order {
			thread, ok := s.session.Memory.Threads[threadID]
			if !ok {
				continue
			}

			// Format Header
			historyBuilder.WriteString(fmt.Sprintf("\n--- Conversation Thread: %s ---\n", thread.Label))
			if thread.Category != "" {
				historyBuilder.WriteString(fmt.Sprintf("Category: %s\n", thread.Category))
			}
			if thread.ContextNotes != "" {
				historyBuilder.WriteString(fmt.Sprintf("Context: %s\n", thread.ContextNotes))
			}

			// Format Body (Exchanges)
			historyBuilder.WriteString(fmt.Sprintf("Root: %s\n", thread.RootPrompt))
			for _, interaction := range thread.Exchanges {
				roleName := "User"
				if interaction.Role == RoleAssistant {
					roleName = "Copilot"
				} else if interaction.Role == RoleSystem {
					roleName = "System"
				}
				historyBuilder.WriteString(fmt.Sprintf("%s: %s\n", roleName, interaction.Content))
			}

			// Format Conclusion
			if thread.Conclusion != "" {
				historyBuilder.WriteString(fmt.Sprintf("Conclusion: %s\n", thread.Conclusion))
			}
			historyBuilder.WriteString("--------------------------------\n")
		}

		historyText = historyBuilder.String()
		if historyText != "" {
			historyText = "\n\n[Existing Conversation Threads]\n" + historyText
		}
	}

	fullPrompt := fmt.Sprintf("%s\n\nContext:\n%s%s\n\nUser Query: %s", systemPrompt, contextText, historyText, query)
	if s.EnableObfuscation {
		// Log?
	}

	// DEBUG: Log the full prompt to understand context contamination
	log.Info("Service.Ask: Full Prompt", "prompt", fullPrompt)

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
	_ = toolRecorded

	// Check for Raw Tool Call (from DataAdmin or similar generators)
	if output.Raw != nil {
		if _, err := json.Marshal(output.Raw); err == nil {
			// REMOVED: Drafting Logic for Raw Tool Call
			toolRecorded = true
		}
	}

	// 5. Check for Tool Execution (Agent -> App)
	// If the generator returns a JSON tool call, and we have an executor, run it.
	// NOTE: If toolRecorded is TRUE (Raw output was present), we still check Text because
	// some generators might provide both. But usually Raw takes precedence.
	// However, the test "TestService_Ask_CapturesLastStep_OnToolExecution" relies on parsing Text JSON.
	if executor, ok := ctx.Value(ai.CtxKeyExecutor).(ai.ToolExecutor); ok && executor != nil {
		// Simple heuristic: If output looks like a JSON tool call
		text := strings.TrimSpace(output.Text)
		// Remove markdown code blocks if present
		text = strings.TrimPrefix(text, "```json")
		text = strings.TrimPrefix(text, "```")
		text = strings.TrimSuffix(text, "```")
		text = strings.TrimSpace(text)

		possibleTool := false
		if strings.HasPrefix(text, "{") && strings.Contains(text, "\"tool\"") {
			possibleTool = true
		}

		// Also check toolRecorded (Raw output) - if so, we can use that if Text parsing fails or instead.
		// Detailed logic:
		// 1. Try to parse Text as Tool Call.
		// 2. If Text is NOT valid tool call, but output.Raw IS, use output.Raw.

		// Let's stick to existing logic but just fix the indentation/flow.
		if possibleTool {
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

				// Capture Last Step Logic
				// If we are executing a tool, we should update the LastStep in the session.
				// This is crucial for "run previous step" or "save as script" features.
				s.session.LastStep = &ai.ScriptStep{
					Type:    "command",
					Command: toolCall.Tool,
					Args:    toolCall.Args,
				}
				// Also update CurrentScript to "Drafting" state implicitly (or assume UI handles it)
				if s.session.CurrentScript != nil {
					s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, *s.session.LastStep)
				}
				toolRecorded = true

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
				} else if s.session.Transaction != nil {
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

				// [Fix] Update Memory Context (Prevent Amnesia on Tool Execution)
				// We must record the interaction even if it was a direct tool execution.
				if s.session.Memory != nil {
					thread := s.session.Memory.GetCurrentThread()
					// If new topic or no thread active, create one
					if topicAssessment.IsNewTopic || thread == nil {
						newID := sop.NewUUID()
						newThread := &ConversationThread{
							ID:     newID,
							Label:  topicAssessment.NewTopicLabel,
							Status: "active",
						}
						if newThread.Label == "" {
							newThread.Label = "Conversation"
						}
						// Capture Root Prompt
						newThread.RootPrompt = query

						s.session.Memory.AddThread(newThread)
						s.session.Memory.PromoteThread(newID)
						thread = newThread
					}

					thread.Exchanges = append(thread.Exchanges, Interaction{
						Role:      RoleUser,
						Content:   query,
						Timestamp: time.Now().Unix(),
					})

					thread.Exchanges = append(thread.Exchanges, Interaction{
						Role:      RoleAssistant,
						Content:   fmt.Sprintf("Tool '%s' executed.\nResult: %s", toolCall.Tool, result),
						Timestamp: time.Now().Unix(),
					})
				}

				return result, nil
			}

			// Fallback: If JSON parsing failed (maybe invalid JSON), return as is
			// Or continue to non-tool processing
			// For now, we continue to non-tool processing (just text response)
			// return text, nil
		}
	}

	// De-obfuscate Output Text
	finalText := output.Text
	if s.EnableObfuscation {
		finalText = obfuscation.GlobalObfuscator.DeobfuscateText(output.Text)
	}

	// Record Chat Output if recording
	if s.session.CurrentScript != nil && !toolRecorded {
		s.session.LastStep = &ai.ScriptStep{
			Type:   "ask",
			Prompt: query,
		}
		s.session.CurrentScript.Steps = append(s.session.CurrentScript.Steps, *s.session.LastStep)
	}

	// Update Session Memory (Structured)
	// We rely on the TopicAssessment performed at the start of the request.

	// If assessment said New Topic, we create one.
	// If assessment said Existing Topic, PromoteThread was already called, so CurrentThreadID is set.

	var currentThread *ConversationThread

	// Defensive check: If IdentifyTopic selected a thread that doesn't exist (hallucination?), force new.
	if !topicAssessment.IsNewTopic && topicAssessment.TopicUUID != "" {
		currentThread = s.session.Memory.GetCurrentThread()
		// If nil, it means the ID was invalid or not found, fall back to new.
	}

	if currentThread == nil {
		newThreadID := sop.NewUUID()
		label := "New Topic"
		if topicAssessment.NewTopicLabel != "" {
			label = topicAssessment.NewTopicLabel
		} else if len(query) > 0 {
			label = query[:min(len(query), 20)] + "..."
		}

		newThread := &ConversationThread{
			ID:         newThreadID,
			RootPrompt: query,
			Label:      label,
			Category:   "General",
			Exchanges:  make([]Interaction, 0),
			Status:     "active",
		}
		s.session.Memory.AddThread(newThread)
		currentThread = newThread
	}

	// Add User Interaction
	currentThread.Exchanges = append(currentThread.Exchanges, Interaction{
		Role:      RoleUser,
		Content:   query,
		Timestamp: time.Now().Unix(),
	})

	// Add Assistant Interaction
	assistantContent := finalText
	if toolRecorded {
		assistantContent = fmt.Sprintf("(Tool Execution) %s", finalText)
	}
	currentThread.Exchanges = append(currentThread.Exchanges, Interaction{
		Role:      RoleAssistant,
		Content:   assistantContent,
		Timestamp: time.Now().Unix(),
	})

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
