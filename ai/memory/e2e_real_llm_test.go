//go:build llm
// +build llm

package memory_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/embed"
	"github.com/sharedcode/sop/ai/generator"
	"github.com/sharedcode/sop/ai/memory"
	"github.com/sharedcode/sop/inmemory"
)

type Event struct {
	ID        string
	Text      string
	Timestamp time.Time
}

func (e Event) GetText() string {
	return e.Text
}

func TestActiveMemory_EndToEnd(t *testing.T) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		apiKey = os.Getenv("LLM_API_KEY")
	}
	if apiKey == "" {
		t.Skip("Skipping End-to-End LLM Active Memory Test; GEMINI_API_KEY or LLM_API_KEY not provided.")
	}

	model := os.Getenv("GEMINI_MODEL")
	if model == "" {
		model = "gemini-2.5-flash"
	}

	// 1. Initialize real LLM (Generator)
	gen, err := generator.New("gemini", map[string]any{
		"api_key": apiKey,
		"model":   model,
	})
	if err != nil {
		t.Fatalf("Failed to initialize Gemini generator: %v", err)
	}

	// 2. Initialize real Embedder (Use default Gemini embedding model)
	emb := embed.NewGemini(apiKey, "")

	// 3. Initialize O(1) SOP In-Memory Backend and Active Memory B-Tree
	bti := inmemory.NewBtree[memory.VectorKey, memory.Vector](true)
	sti := inmemory.NewBtree[sop.UUID, memory.Item[Event]](true)
	cti := inmemory.NewBtree[sop.UUID, *memory.Category](true)

	memStore := memory.NewStore[Event](cti.Btree, bti.Btree, sti.Btree)

	// 4. Initialize The Butler (Memory Manager & Knowledge Base)
	manager := memory.NewMemoryManager[Event](memStore, gen, emb)
	kb := &memory.KnowledgeBase[Event]{
		Manager: manager,
		BaseKnowledgeBase: memory.BaseKnowledgeBase[Event]{
			Store:    memStore,
		},
	}
	
	ctx := context.Background()

	// --- PHASE 1: "DAYTIME" (Short-Term Memory Buffer) ---
	t.Log("===> PHASE 1: User specifies a strong rule regarding DB admin <===")
	ruleText := "Always remember: Database production queries must ONLY be executed after 10 PM. Never query production during daytime business hours."

	event := Event{
		ID:        "evt-001",
		Summaries: []string{      ruleText,
		Timestamp: time.Now(),
	}

	// Ingesting acts as writing to the Scratchpad (O(1) buffer)
	thoughts := []memory.Thought[Event]{
		{Summaries: []string{ ruleText, Data: event},
	}
	err = kb.IngestThoughts(ctx, thoughts, "Database Administrator")
	if err != nil {
		t.Fatalf("Failed to ingest Day 1 thought: %v", err)
	}
	t.Log("Successfully ingested Day 1 thought into Short-Term Memory.")

	// --- PHASE 2: "SLEEP CYCLE" (Categorization & Long-Term Embeds) ---
	t.Log("===> PHASE 2: Simulating Sleep Cycle <===")
	err = manager.SleepCycle(ctx)
	if err != nil {
		t.Fatalf("Sleep Cycle failed: %v", err)
	}
	t.Log("Sleep cycle complete. LLM categorized the thought into LTM.")

	// --- PHASE 3: "NEXT MORNING" (Recall/RAG using R-Tree logic) ---
	t.Log("===> PHASE 3: User asks a related question next morning <===")
	queryText := "What time am I allowed to query the production database?"

	// Ask the embedder for the vector of the new question
	queryVecs, err := emb.EmbedTexts(ctx, []string{queryText})
	if err != nil || len(queryVecs) == 0 {
		t.Fatalf("Failed to embed morning query: %v", err)
	}

	// Search LTM 
	opts := &memory.SearchOptions[Event]{
		Limit: 1, // Get top 1 result
	}
	
	hits, err := memStore.Query(ctx, queryVecs[0], opts)
	if err != nil {
		t.Fatalf("Failed to query LongTermMemory: %v", err)
	}

	// Verification
	if len(hits) == 0 {
		t.Fatalf("The Butler failed to recall the database administration rule from LongTermMemory!")
	}

	retrievedEvent := hits[0].Payload
	t.Logf("The Butler remembered: %q (Distance: %f)", retrievedEvent.Text, hits[0].Score)
	
	// Ensure the semantic hit is valid
	if retrievedEvent.Text != ruleText {
		t.Errorf("Expected retrieved text to be %q, got %q", ruleText, retrievedEvent.Text)
	}
}
