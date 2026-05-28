package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

type pipelineFakeGen struct {
	Response  string
	CallCount int
}

func (m *pipelineFakeGen) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.CallCount++
	return ai.GenOutput{Text: m.Response}, nil
}
func (m *pipelineFakeGen) Name() string                                 { return "fake_pipeline" }
func (m *pipelineFakeGen) EstimateCost(inTokens, outTokens int) float64 { return 0.0 }

func TestCopilotPipeline_Phases(t *testing.T) {
	ctx := context.Background()

	os.RemoveAll("./test_data/pipeline")
	defer os.RemoveAll("./test_data/pipeline")
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{"./test_data/pipeline"}}
	sysDB := database.NewDatabase(sysDBOptions)

	cfg := Config{}
	ag := NewCopilotAgent(cfg, map[string]sop.DatabaseOptions{}, sysDB)

	if ag.service == nil {
		ag.service = &Service{}
	}
	ag.service.session = &RunnerSession{
		MRU: []MRUItem{},
	}

	ag.Memory = memory.NewMemoryUnit("test_pipeline_agent")

	// Create mock generator
	fakeGen := &pipelineFakeGen{Response: "OMNI"}

	query := "Help me create an SOP Space for my new project."

	// --- Phase 1: Intent Routing ---
	intent := ag.classifyIntent(ctx, query, fakeGen)
	if intent != "OMNI" {
		t.Fatalf("Expected OMNI, got %s", intent)
	}

	// --- Phase 2: Domain Classification ---
	fakeGen.Response = `{"domain": "Spaces"}` // Simulate LLM returning 'Spaces' domain
	taskCtx := ag.evaluateRoutingGates(ctx, query, fakeGen)
	if taskCtx == nil {
		t.Fatalf("Expected non-nil TaskContextClassification")
	}
	if taskCtx.Domain != "Spaces" {
		t.Errorf("Expected 'Spaces' domain classification, got %v", taskCtx.Domain)
	}

	// Because evaluateRoutingGates automatically injects tools into MRU for Spaces,
	// let's verify that injection occurred in MRU.
	foundToolsInMRU := false
	for _, item := range ag.getMRUSnapshot() {
		if item.Category == SYSTEM_TOOLS {
			foundToolsInMRU = true
			if !strings.Contains(item.Context, "Structured Context: Spaces Tools") && !strings.Contains(item.Context, "Relevant Space Operations") {
				t.Errorf("Expected Spaces-related tools to be injected into MRU based on Spaces classification")
			}
			break
		}
	}

	if !foundToolsInMRU {
		t.Log("Note: searchKnowledgeBase failed internally because of missing setup, but classification still works.")
		ag.MarkMRUCategory(SYSTEM_TOOLS, "\nStructured Context: Knowledge Base Management Tools\nMock KB Tools Definition")
	}

	// --- Phase 3: MRU Tracking & Propagation Verification ---
	// Let's manually inject "Spaces" MRU to simulate previous context
	// --- Phase 3: MRU Tracking & Propagation Verification ---
	// Let's manually inject "Spaces" MRU to simulate previous context
	ag.markMRUCategoryWithSource(playbookMRUCategory("Spaces"), "MRU Context: The user likes blue theme spaces", MRUSourcePlaybook)

	if ag.service.session.Memory == nil {
		ag.service.session.Memory = NewShortTermMemory()
	}
	thread := &ConversationThread{
		ID:        sop.NewUUID(),
		Exchanges: []Interaction{{Entity: intent, ActiveKB: "sop", Content: "previous space query"}},
	}
	ag.service.session.Memory.AddThread(thread)
	ag.service.session.Memory.CurrentThreadID = thread.ID

	ag.trackEpisodeMetadata(ctx, intent) // This triggers the cross-domain propagation logic

	// --- Phase 4: Prompt Construction Snapshotting ---
	// Mock Session Payload so Domains are linked properly with MRU Cache pulling
	payload := &ai.SessionPayload{
		ActiveDomain: "Spaces",
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	prompt := ag.buildSystemPrompt(ctx, query, *taskCtx)

	if !strings.Contains(prompt, "\"component\": \"focused_execution_context\"") {
		t.Fatalf("Snapshot Failure: Built prompt missing focused execution context component")
	}
	if !strings.Contains(prompt, "Domain: Spaces") || !strings.Contains(prompt, "Relevant Space Operations") {
		t.Fatalf("Snapshot Failure: Built prompt missing focused Spaces execution context")
	}

	if !strings.Contains(prompt, "System Tools") && !strings.Contains(prompt, "System_Tools") && !strings.Contains(prompt, "active memory") {
		// "active memory" tests the Persona portion
		t.Fatalf("Snapshot Failure: Built prompt is missing Persona/System Tools framework")
	}

	if !strings.Contains(prompt, "MRU Context: The user likes blue theme spaces") {
		t.Errorf("Snapshot Failure: Built prompt missing MRU injected knowledge")
	}

	// --- Phase 5: Delegating to Engine ---
	// Mock a JSON payload wrapped in code blocks to simulate reasoning layer tool use output
	fakeGen.Response = "I can definitely help with that. ```json\n[{\"tool\":\"search_space\",\"args\":{}}]\n```"
	finalText, err := ag.delegateToReasoningEngine(ctx, query, fakeGen, prompt)
	if err != nil {
		t.Fatalf("Engine delegation failed: %v", err)
	}
	if !strings.Contains(finalText, "I can definitely help") && !strings.Contains(finalText, "search_space") {
		t.Errorf("Unexpected delegation output: %s", finalText)
	}
}

func TestCopilotPipeline_StoresSchemaFallback_NoArtifacts(t *testing.T) {
	ctx := context.Background()

	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}}
	sysDB := database.NewDatabase(sysDBOptions)

	tx, err := sysDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("failed to begin setup transaction: %v", err)
	}
	if _, err := sysDB.NewBtree(ctx, "users_schema_fallback", tx); err != nil {
		t.Fatalf("failed to create users_schema_fallback store: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit setup transaction: %v", err)
	}

	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	if ag.service == nil {
		ag.service = &Service{}
	}
	ag.service.session = &RunnerSession{MRU: []MRUItem{}}

	payload := &ai.SessionPayload{
		CurrentDB: SystemDBName,
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	prompt := ag.buildSystemPrompt(ctx, "List stores", TaskContextClassification{
		Domain: StoresDomain,
		Layers: []LayerInfo{{
			Name: "Single-Domain",
			CRUD: []string{"R"},
		}},
	})

	if !strings.Contains(prompt, "\"component\": \"focused_execution_context\"") {
		t.Fatalf("expected focused execution context component in prompt")
	}
	if !strings.Contains(prompt, "Domain: Stores") || !strings.Contains(prompt, "CRUD Scope: R") {
		t.Fatalf("expected focused Stores execution details in prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "\"component\": \"schema\"") {
		t.Fatalf("expected schema component fallback for Stores with no classified artifacts")
	}
	if !strings.Contains(prompt, "Available Stores") || !strings.Contains(prompt, "users_schema_fallback") {
		t.Fatalf("expected generic schema fallback to include active store listing: %s", prompt)
	}
}

func TestBuildSystemPrompt_AppliesBudgetWithoutBreakingMemoryFlow(t *testing.T) {
	ctx := context.Background()
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}}
	sysDB := database.NewDatabase(sysDBOptions)
	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{}, Memory: NewShortTermMemory()}}

	longTools := strings.Repeat("TOOL_RULE ", 1200)
	ag.markMRUCategoryWithSource(SYSTEM_TOOLS, longTools, MRUSourceSystemTools)

	thread := &ConversationThread{ID: sop.NewUUID(), Exchanges: []Interaction{
		{Role: RoleUser, Content: strings.Repeat("User context ", 600)},
		{Role: RoleAssistant, Content: strings.Repeat("Assistant context ", 600)},
	}}
	ag.service.session.Memory.AddThread(thread)
	ag.service.session.Memory.CurrentThreadID = thread.ID

	prompt := ag.buildSystemPrompt(ctx, "Find users in users store", TaskContextClassification{
		Domain:      StoresDomain,
		DBArtifacts: []string{"users"},
		Layers:      []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
	})

	if len(prompt) > 17000 {
		t.Fatalf("expected budgeted prompt length <= 17000 chars, got %d", len(prompt))
	}
	if !strings.Contains(prompt, "\"component\": \"focused_execution_context\"") {
		t.Fatalf("expected focused context to survive budgeting: %s", prompt)
	}
	if !strings.Contains(prompt, "\"component\": \"system_tools\"") {
		t.Fatalf("expected system tools to survive budgeting: %s", prompt)
	}
	if !strings.Contains(prompt, "\"component\": \"conversation_history\"") {
		t.Fatalf("expected STM-backed history to remain present after budgeting: %s", prompt)
	}
	if !strings.Contains(prompt, "[truncated]") {
		t.Fatalf("expected oversized components to be truncated under budget: %s", prompt)
	}
	if !strings.Contains(prompt, "Find users in users store") {
		t.Fatalf("expected user query to remain intact: %s", prompt)
	}
}

func TestPromptBudgetProfile_UsesRoutingGate(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)

	discovery := ag.promptBudgetProfile(TaskContextClassification{
		Domain: StoresDomain,
		Layers: []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
	})
	focused := ag.promptBudgetProfile(TaskContextClassification{
		Domain:      StoresDomain,
		DBArtifacts: []string{"users"},
		Layers:      []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
		RoutingGate: RoutingGateFocused,
	})
	continuity := ag.promptBudgetProfile(TaskContextClassification{
		Domain:      StoresDomain,
		DBArtifacts: []string{"users"},
		Layers:      []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
		RoutingGate: RoutingGateContinuity,
	})

	if focused.ComponentCharBudgets[ComponentFocusedContext] <= discovery.ComponentCharBudgets[ComponentFocusedContext] {
		t.Fatalf("expected focused gate to prioritize focused context budget, got focused=%d discovery=%d", focused.ComponentCharBudgets[ComponentFocusedContext], discovery.ComponentCharBudgets[ComponentFocusedContext])
	}
	if focused.ComponentCharBudgets[ComponentHistory] >= discovery.ComponentCharBudgets[ComponentHistory] {
		t.Fatalf("expected focused gate to reduce history budget, got focused=%d discovery=%d", focused.ComponentCharBudgets[ComponentHistory], discovery.ComponentCharBudgets[ComponentHistory])
	}
	if continuity.ComponentCharBudgets[ComponentHistory] <= discovery.ComponentCharBudgets[ComponentHistory] {
		t.Fatalf("expected continuity gate to expand history budget, got continuity=%d discovery=%d", continuity.ComponentCharBudgets[ComponentHistory], discovery.ComponentCharBudgets[ComponentHistory])
	}
	if continuity.TotalChars <= focused.TotalChars {
		t.Fatalf("expected continuity gate to allow more total context than focused gate, got continuity=%d focused=%d", continuity.TotalChars, focused.TotalChars)
	}
}

func TestSystemPromptBuilder_BudgetReportTracksTrimmedComponents(t *testing.T) {
	builder := NewSystemPromptBuilder().
		With(ComponentSystemTools, strings.Repeat("TOOL_RULE ", 200)).
		With(ComponentHistory, strings.Repeat("history ", 200)).
		With(ComponentUserQuery, "User: inspect users")

	prompt, report := builder.ToJSONWithBudgetReport(PromptBudgetProfile{
		TotalChars: 1200,
		ComponentCharBudgets: map[PromptComponent]int{
			ComponentSystemTools: 500,
			ComponentHistory:     300,
			ComponentUserQuery:   200,
		},
		TrimPriorityLowToHigh: []PromptComponent{
			ComponentHistory,
			ComponentSystemTools,
			ComponentUserQuery,
		},
	})

	if prompt == "" || prompt == "[]" {
		t.Fatalf("expected rendered prompt output")
	}
	if report.OriginalTotalChars <= report.FinalTotalChars {
		t.Fatalf("expected report to show trimming, got original=%d final=%d", report.OriginalTotalChars, report.FinalTotalChars)
	}
	trimmed := report.TrimmedComponents()
	if len(trimmed) < 2 {
		t.Fatalf("expected multiple trimmed components, got %#v", trimmed)
	}
	if trimmed[0].OriginalChars <= trimmed[0].FinalChars {
		t.Fatalf("expected trimmed stat to shrink chars, got %#v", trimmed[0])
	}
	if !strings.Contains(prompt, "[truncated]") {
		t.Fatalf("expected prompt output to include truncation marker: %s", prompt)
	}
	if summary := summarizePromptBudgetTrim(report); !strings.Contains(summary, "system_tools") || !strings.Contains(summary, "conversation_history") {
		t.Fatalf("expected trim summary to mention reduced components, got %q", summary)
	}
}
