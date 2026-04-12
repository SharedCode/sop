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

	// Verify we can open the store as a Vector DB
	db := database.NewDatabase(opts)
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		t.Fatalf("Failed to open read transaction: %v", err)
	}

	_, err = db.OpenVectorStore(ctx, "medical_kb", trans, vector.Config{UsageMode: ai.BuildOnceQueryMany})
	if err != nil {
		trans.Rollback(ctx)
		t.Fatalf("Expected medical_kb VectorStore to exist, got error: %v", err)
	}
	trans.Commit(ctx)
}
