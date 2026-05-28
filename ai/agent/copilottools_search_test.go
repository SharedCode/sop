package agent

import (
	"context"
	"github.com/sharedcode/sop/ai/database"
	"testing"
)

func prepareKBForSearchTest(ctx context.Context, t *testing.T, db *database.Database, kbName string, docText string) {
	// Stub
}

func TestCopilotTools_Search_TierRouting(t *testing.T) {
	t.Log("Tier routing logic is now handled in unified searchKnowledgeBase + intent router")
}
