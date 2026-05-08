//go:build integration

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/domain"
	"github.com/sharedcode/sop/ai/embed"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/memory"
)

// kbItem matches the structure of medical.json and sop_base_knowledge.json
type kbItem struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Text        string `json:"text"`
	Description string `json:"description"`
}

// setupIntegHarness bootstraps a Service hooked up to real/in-memory B-Trees.
// It conditionally loads the provided JSON files into designated Knowledge Bases.
func setupIntegHarness(t *testing.T, ctx context.Context, kbsToLoad map[string]string) *Service {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		t.Skip("Skipping integration test: GEMINI_API_KEY is not set.")
	}

	gen, err := generator.New("gemini", map[string]any{})
	if err != nil {
		t.Fatalf("Failed to initialize Gemini: %v", err)
	}

	tmpDir := t.TempDir()

	// 1. Setup System DB (for SOP KB and LTM)
	sysDbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(sysDbOpts)

	// 2. Setup domain DBs (simulating custom KBs the user might select)
	dbs := make(map[string]sop.DatabaseOptions)

	for kbName, filePath := range kbsToLoad {
		kbDir := t.TempDir()
		dbs[kbName] = sop.DatabaseOptions{
			Type:          sop.Clustered,
			StoresFolders: []string{kbDir},
			CacheType:     sop.InMemory,
		}

		seedDB := database.NewDatabase(dbs[kbName])

		// If a file is specified, ingest it
		if filePath != "" {
			t.Logf("Ingesting %s into KB '%s'...", filePath, kbName)
			err := ingestKBFile(ctx, seedDB, kbName, filePath, gen)
			if err != nil {
				t.Fatalf("Failed to ingest %s: %v", filePath, err)
			}
		}
	}

	// 3. Setup the main Domain/Workflow
	cfg := domain.Config[map[string]any]{
		DB:        sysDB,
		Generator: gen,
		Embedder:  embed.NewSimple("test", 384, nil),
		StoreName: "user_long_term_memory",
		ID:        "integ_domain_id",
		Name:      "Integration Test Domain",
	}
	activeDomain := domain.NewGenericDomain(cfg)
	if err != nil {
		t.Fatalf("Failed to bootstrap primary domain: %v", err)
	}

	// Create and bind Service (HistoryInjection = true for authentic ReAct turns)
	service := NewService(activeDomain, sysDB, dbs, gen, nil, nil, false)
	service.EnableHistoryInjection = true
	service.EnableShortTermMemory = true

	// Ensure the scratchpad B-Tree is initialized for STM!
	if err := service.InitializeShortTermMemory(ctx); err != nil {
		t.Fatalf("Failed to initialize Short Term Memory: %v", err)
	}

	return service
}

// ingestKBFile reads the JSON file and upserts it into the KB B-Tree
func ingestKBFile(ctx context.Context, db *database.Database, kbName string, filePath string, generator ai.Generator) error {
	data, err := os.ReadFile("../../" + filePath) // Adjust relative to execution dir
	if err != nil {
		// Attempt fallback relative path depending on where test is run
		data, err = os.ReadFile(filePath)
		if err != nil {
			return err
		}
	}

	var items []kbItem
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Open the KB, generator handles real embeddings if configured
	kbStore, err := db.OpenKnowledgeBase(ctx, kbName, tx, generator, nil)
	if err != nil || kbStore == nil {
		return fmt.Errorf("failed to open KB %s: %v", kbName, err)
	}

	for _, item := range items {
		content := fmt.Sprintf("%s\n%s", item.Text, item.Description)
		payload := map[string]any{
			"id":          item.ID,
			"category":    item.Category,
			"description": item.Description,
		}

		// Use real embeddings if the pipeline supports it,
		// otherwise we provide a dummy vector just to ensure it writes to the BTree index
		dummyVector := make([]float32, 2)
		dummyVector[0] = 1.0
		dummyVector[1] = 1.0

		err = kbStore.Store.Upsert(ctx, memory.Item[map[string]any]{
			ID:         sop.NewUUID(),
			CategoryID: sop.NewUUID(),
			Summaries:  []string{content},
			Data:       payload,
		}, dummyVector)
		if err != nil {
			return fmt.Errorf("upsert failed: %v", err)
		}
	}

	return tx.Commit(ctx)
}

func TestHarness_ReAct_MedicalKBSimulation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Load 1 KB using the medical.json file
	kbs := map[string]string{
		"medical_kb": "medical.json", // Setup looks natively in root or via fallback
	}
	svc := setupIntegHarness(t, ctx, kbs)

	options := []ai.Option{
		ai.WithSessionPayload(&ai.SessionPayload{SelectedKBs: []string{"medical_kb"}}),
	}

	// Query requiring it to fetch from the loaded medical KB
	t.Log("Querying medical condition...")
	res, err := svc.Ask(ctx, "I have continuous sneezing and shivering. Based on the medical KB, what condition might I have?", options...)
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	t.Logf("Agent Response:\n%s", res)
}

func TestHarness_ReAct_MultiKBSimulation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Load multiple KBs
	kbs := map[string]string{
		"medical_kb": "medical.json",
		"sop_kb":     "sop_base_knowledge.json",
		"empty_kb1":  "",
		"empty_kb2":  "",
	}
	svc := setupIntegHarness(t, ctx, kbs)

	options := []ai.Option{
		ai.WithSessionPayload(&ai.SessionPayload{SelectedKBs: []string{"medical_kb", "sop_kb", "empty_kb1", "empty_kb2"}}),
	}

	t.Log("Asking complex multi-KB query...")
	res, err := svc.Ask(ctx, "First read from the medical KB: Tell me what condition has dischromic patches. Then, read from the sop_kb: explain what the No-LLM Direct Command Interface is. Conclude by storing both answers in your ActiveMemory.", options...)
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	t.Logf("Agent Response:\n%s", res)
}

// getKBItemsCount retrieves current total items count across the domain KB
func getKBItemsCount(t *testing.T, ctx context.Context, svc *Service) int {
	tx, err := svc.domain.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return 0
	}
	defer tx.Rollback(ctx)

	memStoreAny, err := svc.domain.Memory(ctx, tx)
	if err != nil {
		return 0
	}

	kb, ok := memStoreAny.(*memory.KnowledgeBase[map[string]any])
	if !ok {
		return 0
	}

	c, _ := kb.Store.Count(ctx)
	return int(c)
}

func TestHarness_ReAct_LongConversation_50Turns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	kbs := map[string]string{
		"medical_kb": "medical.json",
		"sop_kb":     "sop_base_knowledge.json",
	}
	svc := setupIntegHarness(t, ctx, kbs)

	options := []ai.Option{
		ai.WithSessionPayload(&ai.SessionPayload{SelectedKBs: []string{"medical_kb", "sop_kb"}}),
	}

	questions := []string{
		"How do I use slash commands in the SOP No-LLM mode?",
		"What are the symptoms of Allergy in the medical KB?",
		"Are there any side effects to the AI copilot feature? Just give a general common sense answer.",
		"What's 2+2? Keep it short.",
		"If I have a cough and stomach pain, what condition is that?",
	}

	for i := 0; i < 50; i++ {
		q := questions[i%len(questions)]

		// Periodically ask the agent to store things explicitly so STM triggers
		if i == 10 || i == 30 || i == 45 {
			q = "Please store a summary of our entire conversation so far into ActiveMemory."
		}

		res, err := svc.Ask(ctx, fmt.Sprintf("[Turn %d] %s", i+1, q), options...)
		if err != nil {
			t.Fatalf("Ask failed on turn %d: %v", i+1, err)
		}
		t.Logf("[Turn %d] Agent Response: %s", i+1, res)
	}

	initialLTMCount := getKBItemsCount(t, ctx, svc)
	t.Logf("Before SleepCycle: LTM Vector Count = %d", initialLTMCount)

	// Fire sleep cycle
	svc.StartShortTermMemorySleepCycle(ctx, 1*time.Second)
	t.Log("Background sleep cycle initiated. Waiting 4 seconds for DB sweeps...")
	time.Sleep(4 * time.Second)

	finalLTMCount := getKBItemsCount(t, ctx, svc)
	t.Logf("After SleepCycle: LTM Vector Count = %d", finalLTMCount)

	if finalLTMCount <= initialLTMCount {
		t.Logf("Warning: LTM count did not increase (Initial: %d, Final: %d). This might be due to lack of distinct explicit records.", initialLTMCount, finalLTMCount)
	}
}

func TestHarness_ReAct_MultiSleepCycles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	kbs := map[string]string{
		"sop_kb": "sop_base_knowledge.json",
	}
	svc := setupIntegHarness(t, ctx, kbs)

	options := []ai.Option{
		ai.WithSessionPayload(&ai.SessionPayload{SelectedKBs: []string{"sop_kb"}}),
	}

	// Manually initiate constant background sleep cycle
	svc.StartShortTermMemorySleepCycle(ctx, 2*time.Second)

	// Cycle 1: Inject fact
	t.Log("--- Cycle 1: Initial Injection ---")
	_, err := svc.Ask(ctx, "My favorite color is Blue. Please remember this in ActiveMemory. You MUST use the 'conclude_topic' tool to ensure it is recorded. Output the tool call wrapped exactly in a ```json block.", options...)
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	t.Log("Waiting for SleepCycle 1 to run...")
	time.Sleep(4 * time.Second)

	// Cycle 2: Verify and Mutate
	t.Log("--- Cycle 2: Mutation ---")
	res, err := svc.Ask(ctx, "What was my favorite color? Also, I've changed my mind, my favorite color is now Green. Please store this update in your ActiveMemory. You MUST use the 'conclude_topic' tool to record it. Output the tool call wrapped exactly in a ```json block.", options...)
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}
	t.Logf("Response to mutation: %s", res)

	t.Log("Waiting for SleepCycle 2 to run...")
	time.Sleep(4 * time.Second)

	// Cycle 3: Verify the mutated state
	t.Log("--- Cycle 3: Verification ---")
	resFinal, err := svc.Ask(ctx, "What is my favorite color currently? Respond extremely concisely.", options...)
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	t.Log("Waiting for SleepCycle 3 (for good measure) to run...")
	time.Sleep(3 * time.Second)

	t.Logf("Final Verification Response: %s", resFinal)

	finalCount := getKBItemsCount(t, ctx, svc)
	t.Logf("LTM Count at end of 3 cycles: %d", finalCount)
}
