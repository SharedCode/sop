package agent

import (
	"context"

	"github.com/sharedcode/sop/ai/database"
)

// ExportSplitCategoryPathInstruction exports the private function for testing
func ExportSplitCategoryPathInstruction(query string) (string, string) {
	return splitCategoryPathInstruction(query)
}

// ExportSearchKnowledgeBase exports searchKnowledgeBase for testing
func (a *CopilotAgent) ExportSearchKnowledgeBase(ctx context.Context, db *database.Database, kbName string, query string, catPath string, category string, limit int) (string, error) {
	return a.searchKnowledgeBase(ctx, db, kbName, query, catPath, category, limit)
}
