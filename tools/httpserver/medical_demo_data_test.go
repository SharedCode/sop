package main

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
)

func TestPopulateMedicalKnowledgeBase(t *testing.T) {
	ctx := context.Background()
	os.RemoveAll("/tmp/test_medical_db")
	defer os.RemoveAll("/tmp/test_medical_db")

	opts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{"/tmp/test_medical_db"},
	}

	err := PopulateMedicalKnowledgeBase(ctx, opts)
	if err != nil {
		t.Fatalf("Failed to successfully run PopulateMedicalKnowledgeBase: %v", err)
	}

	// Verify we can open the store as a Knowledge Base
	db := database.NewDatabase(opts)
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		t.Fatalf("Failed to open read transaction: %v", err)
	}

	dbEmbedder := GetConfiguredEmbedder(nil)
	dbLLM := GetConfiguredLLM(nil)

	_, err = db.OpenKnowledgeBase(ctx, "medical", trans, dbLLM, dbEmbedder)
	if err != nil {
		trans.Rollback(ctx)
		t.Fatalf("Expected medical_kb KnowledgeBase to exist, got error: %v", err)
	}
	trans.Commit(ctx)
}
