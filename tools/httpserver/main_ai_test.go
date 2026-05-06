package main

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
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

	// Verify we can open the store as a KnowledgeBase instead of a generic B-Tree
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		t.Fatalf("Failed to open read transaction: %v", err)
	}

	dbEmbedder := GetConfiguredEmbedder(nil)
	dbLLM := GetConfiguredLLM(nil)

	_, err = db.OpenKnowledgeBase(ctx, "llm_knowledge", trans, dbLLM, dbEmbedder)
	if err != nil {
		trans.Rollback(ctx)
		t.Fatalf("Expected llm_knowledge KnowledgeBase to exist, got error: %v", err)
	}
	trans.Commit(ctx)
}
