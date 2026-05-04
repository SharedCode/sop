package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/domain"
	"github.com/sharedcode/sop/ai/memory"
	"github.com/sharedcode/sop/ai/vector"
)

type MockE2EGenerator struct{}

func (m *MockE2EGenerator) Name() string { return "mock_generator" }
func (m *MockE2EGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	return ai.GenOutput{Text: "mock_category"}, nil
}
func (m *MockE2EGenerator) EstimateCost(inTokens, outTokens int) float64 { return 0 }

type MockE2EEmbedder struct{}

func (m *MockE2EEmbedder) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i := range texts {
		out[i] = []float32{float32(len(texts[i])) / 100.0, float32(i)}
	}
	return out, nil
}

func (m *MockE2EEmbedder) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	return []float32{float32(len(text)) / 100.0, 1.0}, nil
}

func (m *MockE2EEmbedder) Dim() int {
	return 2
}

func (m *MockE2EEmbedder) Name() string {
	return "MockE2EEmbedder"
}

func TestEndToEndConsolidatorPipeline(t *testing.T) {
	tmpDir := t.TempDir()

	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	db := database.NewDatabase(dbOpts)

	cfg := domain.Config[map[string]any]{
		ID:        "e2e_id",
		Name:      "e2e_domain",
		Embedder:  &MockE2EEmbedder{},
		DB:        db,
		Generator: &MockE2EGenerator{},
		StoreName: "e2e_store",
		StoreCfg: vector.Config{
			UsageMode:             ai.DynamicWithVectorCountTracking,
			EnableIngestionBuffer: true,
			TransactionOptions: sop.TransactionOptions{
				CacheType:     sop.InMemory,
				StoresFolders: []string{tmpDir},
			},
			Cache: sop.GetL2Cache(sop.TransactionOptions{CacheType: sop.InMemory}),
		},
	}
	genericDomain := domain.NewGenericDomain(cfg)

	service := NewService(genericDomain, db, nil, nil, nil, nil, false)
	service.EnableShortTermMemory = true

	ctx, cancel := context.WithCancel(context.Background())

	// DDL Initialization (Ahead of time, synchronous)
	err := service.InitializeShortTermMemory(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize STM: %v", err)
	}

	for i := 0; i < 5; i++ {
		intent := fmt.Sprintf("test-intent-%d", i)
		service.logEpisode(ctx, intent, map[string]string{"foo": "bar"}, "Success", nil)
	}

	// Wait for async workers to finish processing transactions.
	storeName := "user_scratchpad"
	var count int

	for attempt := 0; attempt < 20; attempt++ {
		time.Sleep(500 * time.Millisecond)

		trans, err := db.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			continue
		}

		store, err := db.OpenBtree(ctx, storeName, trans)
		if err != nil {
			trans.Rollback(ctx)
			continue
		}

		ok, _ := store.First(ctx)
		count = 0
		for ok {
			count++
			ok, _ = store.Next(ctx)
		}
		trans.Commit(ctx)

		if count == 6 {
			break
		}
	}

	if count != 6 {
		t.Fatalf("Expected 6 vectors in O(1) buffer, got %d", count)
	}

	/*
		tx.Rollback(ctx)
	*/

	// In memory, TempVectors -> Consolidator needs to execute
	service.StartShortTermMemorySleepCycle(ctx, 500*time.Millisecond)

	t.Log("Waiting 2s for sleep cycle consolidator to process in the background...")
	time.Sleep(2 * time.Second)

	cancel() // Stop sleep cycle

	ctx2 := context.Background()
	tx2, err := genericDomain.BeginTransaction(ctx2, sop.ForReading)
	if err != nil {
		t.Fatalf("Failed to start final read transaction: %v", err)
	}
	memStoreAny, err := genericDomain.Memory(ctx2, tx2)
	if err != nil {
		t.Fatalf("Failed to get memory store: %v", err)
	}

	kb, ok := memStoreAny.(*memory.KnowledgeBase[map[string]any])
	if !ok {
		t.Fatalf("Memory is not KnowledgeBase")
	}

	count2, err := kb.Store.Count(ctx2)
	if err != nil {
		t.Fatalf("Query failed after consolidate: %v", err)
	}
	if count2 < 5 {
		t.Fatalf("Expected 5 vectors survived into Semantic Memory, got %d", count2)
	}
	tx2.Rollback(ctx2)

	// And confirm Optimize succeeds flawlessly on top of these edits
	tx3, _ := genericDomain.BeginTransaction(ctx2, sop.ForWriting)
	idx3, _ := genericDomain.Index(ctx2, tx3)
	err = idx3.Optimize(ctx2)
	if err != nil {
		t.Fatalf("Post-consolidation Optimize() failed: %v", err)
	}

	t.Log("E2E Phase 3 Consolidator pipeline via Agent Service tested perfectly!")
}
