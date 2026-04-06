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
	"github.com/sharedcode/sop/ai/vector"
)

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
		StoreName: "e2e_store",
		StoreCfg:  vector.Config{
			UsageMode: ai.DynamicWithVectorCountTracking,
			TransactionOptions: sop.TransactionOptions{
				CacheType:     sop.InMemory,
				StoresFolders: []string{tmpDir},
			},
			Cache: sop.GetL2Cache(sop.TransactionOptions{CacheType: sop.InMemory}),
		},
	}
	genericDomain := domain.NewGenericDomain(cfg)

	service := NewService(genericDomain, db, nil, nil, nil, nil, false)
	service.EnableActiveMemory = true

	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < 5; i++ {
		intent := fmt.Sprintf("test-intent-%d", i)
		service.logEpisode(ctx, intent, map[string]string{"foo": "bar"}, "Success", nil)
	}

	tx, _ := genericDomain.BeginTransaction(ctx, sop.ForReading)
	idx, _ := genericDomain.Index(ctx, tx)

	results, err := idx.Query(ctx, []float32{0.5, 0.5}, 10, nil)
	if err != nil {
		t.Fatalf("Query failed before consolidate: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("Expected 5 vectors in TempVectors, got %d", len(results))
	}
	tx.Rollback(ctx)

	// In memory, TempVectors -> Consolidator needs to execute
	service.StartActiveMemorySleepCycle(ctx, 500*time.Millisecond)

	t.Log("Waiting 2s for sleep cycle consolidator to process in the background...")
	time.Sleep(2 * time.Second)

	cancel() // Stop sleep cycle

	ctx2 := context.Background()
	tx2, err := genericDomain.BeginTransaction(ctx2, sop.ForReading)
	if err != nil {
		t.Fatalf("Failed to start final read transaction: %v", err)
	}
	idx2, err := genericDomain.Index(ctx2, tx2)
	if err != nil {
		t.Fatalf("Failed to index: %v", err)
	}

	finalResults, err := idx2.Query(ctx2, []float32{0.5, 0.5}, 10, nil)
	if err != nil {
		t.Fatalf("Query failed after consolidate: %v", err)
	}
	if len(finalResults) < 5 {
		t.Fatalf("Expected 5 vectors survived into Semantic Memory, got %d", len(finalResults))
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
