package main

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
)

func TestSeedLLMKnowledge(t *testing.T) {
	ctx := context.Background()
	os.RemoveAll("/tmp/test_seed_llm_db")
	defer os.RemoveAll("/tmp/test_seed_llm_db")

	opts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{"/tmp/test_seed_llm_db"},
	}

	db := database.NewDatabase(opts)

	// Call the generic helper to seed LLM knowledge into the DB
	seedLLMKnowledge(ctx, db)

	// Verify we can open the store as a Vector DB instead of a generic B-Tree
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		t.Fatalf("Failed to open read transaction: %v", err)
	}

	_, err = db.OpenVectorStore(ctx, "llm_knowledge", trans, vector.Config{UsageMode: ai.Dynamic})
	if err != nil {
		trans.Rollback(ctx)
		t.Fatalf("Expected llm_knowledge VectorStore to exist, got error: %v", err)
	}
	trans.Commit(ctx)
}
