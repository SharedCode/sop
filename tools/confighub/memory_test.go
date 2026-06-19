package confighub

import (
	"testing"
)

func TestKnowledgeBase_BasicSearch(t *testing.T) {

	// ctx := context.Background()
	// storagePath := "../../tools/config.json"
	// config, err := core.GetOptions(ctx, storagePath)
	// if err != nil {
	// 	t.Fatalf("GetOptions(%s) failed: %v", storagePath, err)
	// }
	// db := NewDatabase(config)

	// tx, err := db.BeginTransaction(ctx, sop.ForReading)
	// if err != nil {
	// 	t.Fatalf("BeginTransaction failed: %v", err)
	// }

	// kb, err := db.OpenKnowledgeBase(ctx, "tasks3", tx, nil, nil, false)
	// if err != nil {
	// 	t.Fatalf("OpenKnowledgeBase failed: %v", err)
	// }

	// req := []memory.SearchRequest[map[string]any]{{CategoryPath: "Language Bindings/c#", Limit: 5}}
	// hits, err := kb.Search(ctx, req)
	// if err != nil {
	// 	t.Fatalf("KnowledgeBase.Search failed: %v", err)
	// }

	// if len(hits) == 0 {
	// 	//t.Fatalf("KnowledgeBase.Search failed, no hits for %s", req[])
	// }

	// if err := tx.Commit(ctx); err != nil {
	// 	t.Fatalf("Commit failed: %v", err)
	// }

}
