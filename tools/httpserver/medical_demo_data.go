package main

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
)

// PopulateMedicalKnowledgeBase initializes the stores used for the "medical expert" use-case.
func PopulateMedicalKnowledgeBase(ctx context.Context, opts sop.DatabaseOptions) error {
	db := database.NewDatabase(opts)
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Create VectorStore instead of B-Tree for medical data logic.
	_, err = db.OpenVectorStore(ctx, "medical_knowledge_base", trans, vector.Config{UsageMode: ai.BuildOnceQueryMany})
	if err != nil {
		trans.Rollback(ctx)
		return fmt.Errorf("failed to create 'medical_knowledge_base' vector store: %v", err)
	}

	if err := trans.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit medical knowledge base initialization: %v", err)
	}

	return nil
}
