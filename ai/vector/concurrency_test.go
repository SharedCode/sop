package vector_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/cache"
	core_database "github.com/sharedcode/sop/database"
)

func TestOptimize_ConcurrencyLocking(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-concurrent-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize Database
	db := database.NewDatabase(core_database.Standalone, tmpDir)
	tx, err := db.BeginTransaction(context.Background(), sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// We need a cache for locking
	c := cache.NewInMemoryCache()

	idx, err := db.OpenVectorStore(context.Background(), "test_concurrent", tx, vector.Config{
		UsageMode: ai.Dynamic,
		Cache:     c,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Populate with some data so Optimize has something to do
	var items []ai.Item[map[string]any]
	for i := 0; i < 5000; i++ {
		items = append(items, ai.Item[map[string]any]{
			ID:      fmt.Sprintf("item-%d", i),
			Vector:  []float32{float32(i), float32(i)},
			Payload: map[string]any{"val": i},
		})
	}
	if err := idx.UpsertBatch(context.Background(), items); err != nil {
		t.Fatalf("UpsertBatch failed: %v", err)
	}

	// Commit setup
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Re-open for concurrent test
	tx2, err := db.BeginTransaction(context.Background(), sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction 2 failed: %v", err)
	}
	idx2, err := db.OpenVectorStore(context.Background(), "test_concurrent", tx2, vector.Config{
		UsageMode: ai.Dynamic,
		Cache:     c,
	})
	if err != nil {
		t.Fatalf("Open 2 failed: %v", err)
	}

	// Start Optimize in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)

	optimizeErrChan := make(chan error, 1)
	startOptimize := make(chan struct{})
	optimizeDone := make(chan struct{})

	go func() {
		defer wg.Done()
		close(startOptimize) // Signal that we are starting
		if err := idx2.Optimize(context.Background()); err != nil {
			optimizeErrChan <- err
		}
		close(optimizeDone)
	}()

	<-startOptimize
	// Give Optimize a moment to acquire the lock
	time.Sleep(50 * time.Millisecond)

	select {
	case <-optimizeDone:
		t.Fatal("Optimize finished too quickly, increase dataset size")
	default:
	}

	// Try to Upsert using a NEW transaction/instance (simulating another request)
	tx3, err := db.BeginTransaction(context.Background(), sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction 3 failed: %v", err)
	}
	idx3, err := db.OpenVectorStore(context.Background(), "test_concurrent", tx3, vector.Config{
		UsageMode: ai.Dynamic,
		Cache:     c,
	})
	if err != nil {
		t.Fatalf("Open 3 failed: %v", err)
	}

	// This Upsert should FAIL because Optimize is running
	err = idx3.Upsert(context.Background(), ai.Item[map[string]any]{
		ID:     "new-item",
		Vector: []float32{1, 1},
	})

	if err == nil {
		t.Error("Expected Upsert to fail due to active optimization lock, but it succeeded")
	} else {
		t.Logf("Upsert failed as expected: %v", err)
	}

	// Wait for Optimize to finish
	wg.Wait()
	close(optimizeErrChan)
	for err := range optimizeErrChan {
		t.Fatalf("Optimize failed: %v", err)
	}

	// Now Upsert should succeed
	if err := idx3.Upsert(context.Background(), ai.Item[map[string]any]{
		ID:     "new-item-2",
		Vector: []float32{1, 1},
	}); err != nil {
		t.Errorf("Upsert failed after optimization finished: %v", err)
	}
}
