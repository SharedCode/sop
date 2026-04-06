package main

import (
	"context"
	"fmt"

	"cmp"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
)

// PopulateMedicalKnowledgeBase initializes the stores used for the "medical expert" use-case.
func PopulateMedicalKnowledgeBase(ctx context.Context, opts sop.DatabaseOptions) error {
	trans, err := database.BeginTransaction(ctx, opts, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}

	// Just creating the store for now, data will be enhanced later on.
	_, err = database.NewBtree[string, string](ctx, opts, "medical_knowledge_base", trans, cmp.Compare[string])
	if err != nil {
		trans.Rollback(ctx)
		return fmt.Errorf("failed to create 'medical_knowledge_base' store: %v", err)
	}

	if err := trans.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit medical knowledge base initialization: %v", err)
	}

	return nil
}
