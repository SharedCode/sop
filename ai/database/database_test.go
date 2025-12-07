package database_test

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/vector"
	core "github.com/sharedcode/sop/database"
)

func TestAIDatabase_Standalone_ModelStore(t *testing.T) {
	storagePath := "/tmp/sop_test_ai_model"
	_ = os.RemoveAll(storagePath)
	defer os.RemoveAll(storagePath)

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
	storagePath := "/tmp/sop_test_ai_vector"
	_ = os.RemoveAll(storagePath)
	defer os.RemoveAll(storagePath)

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
	storagePath := "/tmp/sop_test_ai_search"
	_ = os.RemoveAll(storagePath)
	defer os.RemoveAll(storagePath)

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
