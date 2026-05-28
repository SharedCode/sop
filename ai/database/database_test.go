package database_test

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
	core "github.com/sharedcode/sop/database"
)

func TestAIDatabase_RemoveKnowledgeBase_RemovesAllDomainStores(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	if _, err := db.OpenKnowledgeBase(ctx, "tasks3", tx, nil, nil, false); err != nil {
		t.Fatalf("OpenKnowledgeBase failed: %v", err)
	}

	if _, err := db.OpenVectorStore(ctx, "tasks3", tx, vector.Config{UsageMode: ai.Dynamic}); err != nil {
		t.Fatalf("OpenVectorStore failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	before, err := db.GetDomains(ctx)
	if err != nil {
		t.Fatalf("GetDomains before delete failed: %v", err)
	}
	if len(before) == 0 {
		t.Fatal("expected tasks3 domain to exist before deletion")
	}

	if err := db.RemoveKnowledgeBase(ctx, "tasks3"); err != nil {
		t.Fatalf("RemoveKnowledgeBase failed: %v", err)
	}

	after, err := db.GetDomains(ctx)
	if err != nil {
		t.Fatalf("GetDomains after delete failed: %v", err)
	}
	for _, name := range after {
		if name == "tasks3" {
			t.Fatalf("expected tasks3 to be removed from domains, got %v", after)
		}
	}

	verifyTx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		t.Fatalf("BeginTransaction verify failed: %v", err)
	}
	stores, err := verifyTx.GetStores(ctx)
	_ = verifyTx.Rollback(ctx)
	if err != nil {
		t.Fatalf("GetStores verify failed: %v", err)
	}
	for _, store := range stores {
		if len(store) >= len("tasks3/") && store[:len("tasks3/")] == "tasks3/" {
			t.Fatalf("expected no remaining tasks3 stores, found %q", store)
		}
	}
}

func TestAIDatabase_RemoveKnowledgeBase_AcceptsDataSuffix(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	if _, err := db.OpenKnowledgeBase(ctx, "tasks4", tx, nil, nil, false); err != nil {
		t.Fatalf("OpenKnowledgeBase failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if err := db.RemoveKnowledgeBase(ctx, "tasks4/data"); err != nil {
		t.Fatalf("RemoveKnowledgeBase with /data suffix failed: %v", err)
	}
}

func TestAIDatabase_RemoveKnowledgeBase_SuggestsCorrectCase(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	if _, err := db.OpenKnowledgeBase(ctx, "Tasks5", tx, nil, nil, false); err != nil {
		t.Fatalf("OpenKnowledgeBase failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	err = db.RemoveKnowledgeBase(ctx, "tasks5")
	if err == nil {
		t.Fatal("expected case-mismatched delete to fail")
	}
	if !strings.Contains(err.Error(), "did you mean 'Tasks5'") {
		t.Fatalf("expected case suggestion, got %v", err)
	}

	after, err := db.GetDomains(ctx)
	if err != nil {
		t.Fatalf("GetDomains after failed delete failed: %v", err)
	}
	found := false
	for _, name := range after {
		if name == "Tasks5" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected Tasks5 to remain after failed delete, got %v", after)
	}
}

func TestAIDatabase_RemoveKnowledgeBase_ReturnsNotFoundWhenMissing(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	err := db.RemoveKnowledgeBase(ctx, "missing_space")
	if err == nil {
		t.Fatal("expected missing knowledge base delete to fail")
	}
}

func TestAIDatabase_Standalone_ModelStore(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	ms, err := db.OpenModelStore(ctx, "models", tx)
	if err != nil {
		t.Fatalf("OpenModelStore failed: %v", err)
	}

	if ms == nil {
		t.Error("ModelStore is nil")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestAIDatabase_Standalone_VectorStore(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	cfg := vector.Config{
		UsageMode: ai.Dynamic,
	}
	vs, err := db.OpenVectorStore(ctx, "vectors", tx, cfg)
	if err != nil {
		t.Fatalf("OpenVectorStore failed: %v", err)
	}

	if vs == nil {
		t.Error("VectorStore is nil")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestAIDatabase_Standalone_Search(t *testing.T) {
	storagePath := t.TempDir()

	db := database.NewDatabase(core.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	idx, err := db.OpenSearch(ctx, "search_index", tx)
	if err != nil {
		t.Fatalf("OpenSearch failed: %v", err)
	}

	if idx == nil {
		t.Error("Search Index is nil")
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}
