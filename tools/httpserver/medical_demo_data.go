package main

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
)

// PopulateMedicalKnowledgeBase initializes the stores used for the "medical expert" use-case.
func PopulateMedicalKnowledgeBase(ctx context.Context, opts sop.DatabaseOptions) error {
	db := database.NewDatabase(opts)
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Create KnowledgeBase instead of B-Tree for medical data logic.
	dbEmbedder := GetConfiguredEmbedder(nil)
	dbLLM := GetConfiguredLLM(nil)

	_, err = db.OpenKnowledgeBase(ctx, "medical", trans, dbLLM, dbEmbedder)
	if err != nil {
		trans.Rollback(ctx)
		return fmt.Errorf("failed to create 'medical' knowledge base: %v", err)
	}

	if err := trans.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit medical knowledge base initialization: %v", err)
	}

	return nil
}
