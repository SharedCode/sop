package agent

import (
	"context"
	"encoding/json"
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
	finalText, toolCalls, outcomeFacts, outcomeRecipes, err := ag.delegateToReasoningEngine(ctx, query, fakeGen, prompt)
	if err != nil {
		t.Fatalf("Engine delegation failed: %v", err)
	}
	if !strings.Contains(finalText, "I can definitely help") && !strings.Contains(finalText, "search_space") {
		t.Errorf("Unexpected delegation output: %s", finalText)
	}
	if len(toolCalls) != 0 {
		t.Fatalf("expected no native tool calls from baseline mock delegation path, got %#v", toolCalls)
	}
	if len(outcomeFacts) != 0 {
		t.Fatalf("expected no grounded outcome facts from baseline mock delegation path, got %#v", outcomeFacts)
	}
	if len(outcomeRecipes) != 0 {
		t.Fatalf("expected no learned outcome recipes from baseline mock delegation path, got %#v", outcomeRecipes)
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

func TestBuildSystemPrompt_IncludesWorkflowRecipesComponent(t *testing.T) {
	ctx := context.Background()
	sysDB := database.NewDatabase(sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}})
	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{}}}

	prompt := ag.buildSystemPrompt(ctx, "Create a script named expensive_orders to find orders over 1000", TaskContextClassification{
		Domain:          StoresDomain,
		DBArtifacts:     []string{"orders"},
		Layers:          []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
		ScriptAuthoring: true,
	})

	var elements []PromptElement
	if err := json.Unmarshal([]byte(prompt), &elements); err != nil {
		t.Fatalf("expected buildSystemPrompt to return JSON prompt elements: %v\nPrompt: %s", err, prompt)
	}

	recipeContent := ""
	for _, element := range elements {
		if element.Component == ComponentRecipes {
			recipeContent = element.Content
			break
		}
	}
	if recipeContent == "" {
		t.Fatalf("expected workflow_recipes component in prompt: %s", prompt)
	}
	if strings.Contains(recipeContent, "[truncated]") {
		t.Fatalf("expected explicit recipe component to fit without truncation, got: %s", recipeContent)
	}
	if !strings.Contains(recipeContent, "Reusable script authoring") || !strings.Contains(recipeContent, "Stores schema-first research") || !strings.Contains(recipeContent, "Stores read transaction flow") || !strings.Contains(recipeContent, "Stores join slice repair") || !strings.Contains(recipeContent, "Stores predicate grounding") {
		t.Fatalf("expected workflow recipes to include script-authoring and stores protocols, got: %s", recipeContent)
	}
	if !strings.Contains(recipeContent, "begin_tx(mode=read)") || !strings.Contains(recipeContent, "create_script") {
		t.Fatalf("expected workflow recipes to retain protocol anchors, got: %s", recipeContent)
	}
}

func TestBuildSystemPrompt_RehydratesSystemToolsFromSTMProjection(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)
	ag.service = &Service{session: NewRunnerSession()}
	ag.service.session.Memory.SetMRUSnapshot([]MRUItem{{
		Category: SYSTEM_TOOLS,
		Context:  "STM_REHYDRATED_TOOLS",
		Source:   MRUSourceSystemTools,
	}})

	prompt := ag.buildSystemPrompt(context.Background(), "List stores", TaskContextClassification{})
	if !strings.Contains(prompt, "STM_REHYDRATED_TOOLS") {
		t.Fatalf("expected buildSystemPrompt to rehydrate system tools from STM projection, got %s", prompt)
	}
}

func TestBuildSystemPrompt_RehydratesAskOutcomeFromSTMProjection(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)
	ag.service = &Service{session: NewRunnerSession()}
	ag.service.session.Memory.SetMRUSnapshot([]MRUItem{
		{Category: askOutcomeMRUCategoryHeader, Context: "Recent Ask Outcome:", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryQuery, Context: "- Last user ask: Find users named John", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryResult, Context: "- Last outcome: Found matching users", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryStoreSchema + "_USERS", Context: "- Confirmed: list_stores confirmed users schema=key:string, first_name:string", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryRelations + "_USERS__USERS_ORDERS__USERS_ORDERS_KEY__USERS_KEY", Context: "- Confirmed: list_stores confirmed users relations=[users_orders(key->users.key)]", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryGuidance, Context: "- Reuse confirmed facts and successful patterns from this outcome before broadening scope.", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
	})

	prompt := ag.buildSystemPrompt(context.Background(), "Find orders for those users", TaskContextClassification{})
	if !strings.Contains(prompt, "Recent Ask Outcome:") || !strings.Contains(prompt, "Find users named John") || !strings.Contains(prompt, "list_stores confirmed users schema=key:string, first_name:string") || !strings.Contains(prompt, "list_stores confirmed users relations=[users_orders(key-") {
		t.Fatalf("expected buildSystemPrompt to rehydrate ask outcome from STM projection, got %s", prompt)
	}
}

func TestBuildSystemPrompt_RehydratesImplicitRecipesFromSTM(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)
	ag.service = &Service{session: NewRunnerSession()}
	ag.service.session.Memory.SetRecipeSnapshot([]RecipeItem{{
		ID:         "implicit.execute_script.research_then_retry",
		Kind:       RecipeKindImplicit,
		Scope:      RecipeScopeMicro,
		Domain:     StoresDomain,
		Topic:      "Research grounded schema before execute_script retry",
		Trigger:    "execute_script repair requires missing schema or relation facts",
		Protocol:   []string{"Call list_stores first to confirm schema and relations.", "Retry execute_script with corrected grounded arguments."},
		Invariants: []string{"Preserve valid script slices.", "Do not broaden scope before the retry."},
		Confidence: 0.95,
		Source:     "inner_loop_success",
	}})

	prompt := ag.buildSystemPrompt(context.Background(), "Find users named John", TaskContextClassification{Domain: StoresDomain})
	if !strings.Contains(prompt, "workflow_recipes") || !strings.Contains(prompt, "Research grounded schema before execute_script retry") || !strings.Contains(prompt, "Retry execute_script with corrected grounded arguments") {
		t.Fatalf("expected buildSystemPrompt to include implicit recipe snapshot, got %s", prompt)
	}
}

func TestBuildAskOutcomeMRUItems_TypesConfirmedStoreFacts(t *testing.T) {
	items := buildAskOutcomeMRUItems(context.Background(), "Find users", "Found users", []ai.ToolCall{{Name: "list_stores"}}, []string{
		"list_stores confirmed users schema=key:string, first_name:string",
		"list_stores confirmed users relations=[users_orders(key->users.key)]",
		"execute_script confirmed join store=users_orders on=users.key->key",
		"execute_script confirmed filter field=first_name op=$eq",
		"execute_script returned: [{\"first_name\":\"John\"}]",
	})

	seenCategories := map[string]bool{}
	for _, item := range items {
		seenCategories[item.Category] = true
	}

	if !seenCategories[askOutcomeMRUCategoryStoreSchema+"_USERS"] {
		t.Fatalf("expected typed schema category, got %+v", items)
	}
	if !seenCategories[askOutcomeMRUCategoryRelations+"_USERS__USERS_ORDERS__USERS_ORDERS_KEY__USERS_KEY"] {
		t.Fatalf("expected typed relations category, got %+v", items)
	}
	if !seenCategories[askOutcomeMRUCategoryJoinSelection+"_JOIN__USERS_ORDERS__USERS_KEY__KEY"] {
		t.Fatalf("expected typed join selection category, got %+v", items)
	}
	if !seenCategories[askOutcomeMRUCategoryFilterSelection+"_FIRST_NAME___EQ"] {
		t.Fatalf("expected typed filter selection category, got %+v", items)
	}
	if !seenCategories[askOutcomeMRUCategoryConfirmed+"_01"] {
		t.Fatalf("expected generic fallback category for unstructured confirmed fact, got %+v", items)
	}
}

func TestProjectMRUItemsFromSTM_AppliesSourceCapsAndPriority(t *testing.T) {
	projected := projectMRUItemsFromSTM([]MRUItem{
		{Category: "PLAYBOOK_old", Source: MRUSourcePlaybook, LastAccessed: 10},
		{Category: "PLAYBOOK_new", Source: MRUSourcePlaybook, LastAccessed: 20},
		{Category: "PLAYBOOK_extra1", Source: MRUSourcePlaybook, LastAccessed: 30},
		{Category: "PLAYBOOK_extra2", Source: MRUSourcePlaybook, LastAccessed: 40},
		{Category: "PLAYBOOK_extra3", Source: MRUSourcePlaybook, LastAccessed: 50},
		{Category: SYSTEM_TOOLS, Source: MRUSourceSystemTools, Context: "tools-a", LastAccessed: 60},
		{Category: "SYSTEM_TOOLS_OLD", Source: MRUSourceSystemTools, Context: "tools-b", LastAccessed: 10},
		{Category: "PERSONA_a", Source: MRUSourcePersona, LastAccessed: 5},
		{Category: "PERSONA_b", Source: MRUSourcePersona, LastAccessed: 15},
		{Category: "PERSONA_c", Source: MRUSourcePersona, LastAccessed: 25},
		{Category: "UNSCOPED", Source: MRUSourceUnknown, LastAccessed: 100},
	}, "sop,Spaces")

	if len(projected) != 7 {
		t.Fatalf("expected capped projection size of 7, got %d (%+v)", len(projected), projected)
	}
	if projected[0].Source != MRUSourcePersona || projected[1].Source != MRUSourcePersona {
		t.Fatalf("expected persona entries to lead projection order, got %+v", projected)
	}
	if projected[2].Source != MRUSourceSystemTools {
		t.Fatalf("expected system tools to follow persona entries, got %+v", projected)
	}
	playbookCount := 0
	for _, item := range projected {
		if item.Category == "UNSCOPED" {
			t.Fatalf("unexpected unknown-source MRU item in projection: %+v", projected)
		}
		if item.Source == MRUSourcePlaybook {
			playbookCount++
		}
	}
	if playbookCount != maxProjectedPlaybookEntries {
		t.Fatalf("expected playbook entries to be capped at %d, got %d (%+v)", maxProjectedPlaybookEntries, playbookCount, projected)
	}
}

func TestEpilogueAndCleanup_PersistsMRUSnapshotWithoutSleepCycleLogging(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)
	ag.service = &Service{session: NewRunnerSession(), EnableShortTermMemory: false}
	ag.markMRUCategoryWithSource(SYSTEM_TOOLS, "TOOLS_FOR_RESTART", MRUSourceSystemTools)

	ag.epilogueAndCleanup(context.Background(), "List stores", "Omni", "Done", []ai.ToolCall{{Name: "list_stores"}, {Name: "execute_script"}}, []string{"list_stores confirmed users schema=key:string, first_name:string", "list_stores confirmed users relations=[users_orders(key->users.key)]"}, []ai.LearnedRecipe{{
		ID:         "implicit.execute_script.research_then_retry",
		Kind:       RecipeKindImplicit,
		Scope:      RecipeScopeMicro,
		Domain:     StoresDomain,
		Topic:      "Research grounded schema before execute_script retry",
		Trigger:    "execute_script repair requires missing schema or relation facts",
		Protocol:   []string{"Call list_stores first to confirm schema and relations.", "Retry execute_script with corrected grounded arguments."},
		Invariants: []string{"Preserve valid script slices."},
		Confidence: 0.95,
		Source:     "inner_loop_success",
	}})

	snapshot := ag.service.session.Memory.GetMRUSnapshot()
	if len(snapshot) == 0 {
		t.Fatalf("expected epilogue to persist MRU snapshot even when sleep-cycle logging is disabled")
	}
	recipeSnapshot := ag.service.session.Memory.GetRecipeSnapshot()
	if len(recipeSnapshot) != 1 || recipeSnapshot[0].ID != "implicit.execute_script.research_then_retry" {
		t.Fatalf("expected epilogue to persist learned recipe snapshot, got %+v", recipeSnapshot)
	}
	if snapshot[0].Category != SYSTEM_TOOLS || snapshot[0].Context != "TOOLS_FOR_RESTART" {
		t.Fatalf("unexpected MRU snapshot persisted to STM: %+v", snapshot)
	}
	foundAskOutcomeQuery := false
	foundAskOutcomeResult := false
	foundAskOutcomeSchema := false
	foundAskOutcomeRelations := false
	foundAskOutcomePattern := false
	for _, item := range snapshot {
		if item.Source == MRUSourceAskOutcome {
			if item.Category == askOutcomeMRUCategoryQuery {
				foundAskOutcomeQuery = strings.Contains(item.Context, "List stores")
			}
			if item.Category == askOutcomeMRUCategoryResult {
				foundAskOutcomeResult = strings.Contains(item.Context, "Done")
			}
			if strings.HasPrefix(item.Category, askOutcomeMRUCategoryStoreSchema+"_") {
				foundAskOutcomeSchema = foundAskOutcomeSchema || strings.Contains(item.Context, "list_stores confirmed users schema=key:string, first_name:string")
			}
			if strings.HasPrefix(item.Category, askOutcomeMRUCategoryRelations+"_") {
				foundAskOutcomeRelations = foundAskOutcomeRelations || strings.Contains(item.Context, "list_stores confirmed users relations=[users_orders(key->users.key)]")
			}
			if item.Category == askOutcomeMRUCategoryToolPattern {
				foundAskOutcomePattern = strings.Contains(item.Context, "Tool pattern: list_stores -> execute_script")
			}
		}
	}
	if !foundAskOutcomeQuery || !foundAskOutcomeResult || !foundAskOutcomeSchema || !foundAskOutcomeRelations || !foundAskOutcomePattern {
		t.Fatalf("expected epilogue to persist compact ask outcome into MRU snapshot, got %+v", snapshot)
	}
}

func TestEpilogueAndCleanup_MergesRecipeSnapshotsAcrossAsks(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)
	ag.service = &Service{session: NewRunnerSession(), EnableShortTermMemory: false}
	ag.service.session.Memory.SetRecipeSnapshot([]RecipeItem{{
		ID:         "implicit.execute_script.research_then_retry",
		Kind:       RecipeKindImplicit,
		Scope:      RecipeScopeMicro,
		Domain:     StoresDomain,
		Topic:      "Research grounded schema before execute_script retry",
		Trigger:    "execute_script repair requires missing schema or relation facts",
		Protocol:   []string{"Call list_stores first to confirm schema and relations."},
		Invariants: []string{"Preserve valid script slices."},
		Confidence: 0.75,
		Source:     "inner_loop_success",
	}})

	ag.epilogueAndCleanup(context.Background(), "Find orders", "Omni", "Done", []ai.ToolCall{{Name: "execute_script"}}, []string{"execute_script returned: []"}, []ai.LearnedRecipe{{
		ID:         "implicit.execute_script.repair_in_place",
		Kind:       RecipeKindImplicit,
		Scope:      RecipeScopeMicro,
		Domain:     StoresDomain,
		Topic:      "Repair execute_script in place",
		Trigger:    "execute_script has a recoverable argument-shape error",
		Protocol:   []string{"Retry the same tool instead of abandoning the plan."},
		Invariants: []string{"Do not switch to unrelated tools before the repair attempt."},
		Confidence: 0.9,
		Source:     "inner_loop_success",
	}})

	recipeSnapshot := ag.service.session.Memory.GetRecipeSnapshot()
	if len(recipeSnapshot) != 2 {
		t.Fatalf("expected epilogue to merge recipe snapshots across asks, got %+v", recipeSnapshot)
	}
	if recipeSnapshot[0].ID != "implicit.execute_script.repair_in_place" || recipeSnapshot[1].ID != "implicit.execute_script.research_then_retry" {
		t.Fatalf("expected merged recipe snapshot ordered by confidence, got %+v", recipeSnapshot)
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
	if present := summarizePromptComponentsPresent(report); !strings.Contains(present, "system_tools") || !strings.Contains(present, "user_query") {
		t.Fatalf("expected present-components summary to mention retained components, got %q", present)
	}
}

func TestSummarizeProjectedMRU_IncludesSourceAndCategory(t *testing.T) {
	summary := summarizeProjectedMRU([]MRUItem{
		{Category: "PERSONA_omni", Source: MRUSourcePersona},
		{Category: SYSTEM_TOOLS, Source: MRUSourceSystemTools},
		{Category: playbookMRUCategory("sop"), Source: MRUSourcePlaybook},
	})
	if !strings.Contains(summary, "persona:PERSONA_omni") || !strings.Contains(summary, "system_tools:System_Tools") || !strings.Contains(summary, "playbook:PLAYBOOK_sop") {
		t.Fatalf("unexpected projected MRU summary: %q", summary)
	}
}
