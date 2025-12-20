package database_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"

	// Ensure Redis adapter is registered for Clustered tests
	_ "github.com/sharedcode/sop/adapters/redis"
)

func TestDatabase_Standalone_Simple(t *testing.T) {
	storagePath := t.TempDir()

	db, _ := database.ValidateOptions(sop.DatabaseOptions{
		StoresFolders: []string{storagePath},
		CacheType:     sop.InMemory,
	})

	ctx := context.Background()
	tx, err := database.BeginTransaction(ctx, db, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	store, err := database.NewBtree[string, string](ctx, db, "test_store", tx, nil)
	if err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}

	if _, err := store.Add(ctx, "key1", "value1"); err != nil {
		t.Errorf("Add failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestDatabase_Standalone_Replication(t *testing.T) {
	// Setup folders for replication
	basePath := t.TempDir()

	folders := []string{
		basePath + "/node1",
		basePath + "/node2",
	}
	for _, f := range folders {
		os.MkdirAll(f, 0755)
	}

	ecConfig := map[string]sop.ErasureCodingConfig{
		"test_store": {
			DataShardsCount:             1,
			ParityShardsCount:           1,
			BaseFolderPathsAcrossDrives: folders,
		},
	}

	db, _ := database.ValidateOptions(sop.DatabaseOptions{
		StoresFolders: []string{folders[0], folders[1]},
		ErasureConfig: ecConfig,
		CacheType:     sop.InMemory,
	})

	ctx := context.Background()
	tx, err := database.BeginTransaction(ctx, db, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Note: Store name must match EC config key or be handled by default?
	// Usually EC config is per store.
	store, err := database.NewBtree[string, string](ctx, db, "test_store", tx, nil)
	if err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}

	if _, err := store.Add(ctx, "key1", "value1"); err != nil {
		t.Errorf("Add failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestDatabase_Clustered_Construction(t *testing.T) {
	// This test verifies we can construct the object.
	// Actual connection might fail if Redis/Cassandra are not present.

	db, err := database.ValidateOptions(sop.DatabaseOptions{
		CacheType:     sop.Redis,
		StoresFolders: []string{t.TempDir()},
	})

	if err != nil {
		t.Fatal("ValidateOptions returned error for Clustered")
	}

	// We expect BeginTransaction to fail or panic if Redis is not reachable,
	// but we can try it to see what happens.
	ctx := context.Background()
	_, err = database.BeginTransaction(ctx, db, sop.ForWriting)
	if err == nil {
		// If it succeeds (maybe mock redis?), great.
		// If it fails, we check if it's a connection error.
		t.Log("BeginTransaction succeeded (unexpected without Redis)")
	} else {
		t.Logf("BeginTransaction failed as expected (no Redis): %v", err)
	}
}

func TestDatabase_Cassandra_Construction(t *testing.T) {
	_, err := database.ValidateCassandraOptions(sop.DatabaseOptions{
		Keyspace:      "test_keyspace",
		StoresFolders: []string{t.TempDir()},
	})

	if err != nil {
		t.Fatal("ValidateCassandraOptions returned error")
	}
}

func TestDatabase_Cassandra_Transaction_Simple(t *testing.T) {
	db, _ := database.ValidateCassandraOptions(sop.DatabaseOptions{
		Keyspace:      "test_keyspace",
		StoresFolders: []string{t.TempDir()},
	})

	ctx := context.Background()
	// Expect error connecting to Cassandra
	_, err := database.BeginTransaction(ctx, db, sop.ForWriting)
	if err == nil {
		t.Fatal("Expected error connecting to Cassandra, got nil")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDatabase_Cassandra_Transaction_Replication(t *testing.T) {
	// Setup folders
	basePath := t.TempDir()

	folders := []string{
		basePath + "/node1",
		basePath + "/node2",
	}
	for _, f := range folders {
		os.MkdirAll(f, 0755)
	}

	ecConfig := map[string]sop.ErasureCodingConfig{
		"test_store": {
			DataShardsCount:             1,
			ParityShardsCount:           1,
			BaseFolderPathsAcrossDrives: folders,
		},
	}

	db, _ := database.ValidateCassandraOptions(sop.DatabaseOptions{
		Keyspace:      "test_keyspace",
		ErasureConfig: ecConfig,
	})

	ctx := context.Background()
	_, err := database.BeginTransaction(ctx, db, sop.ForWriting)
	if err == nil {
		t.Fatal("Expected error connecting to Cassandra, got nil")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDatabase_Setup_GetOptions(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	opts := sop.DatabaseOptions{
		StoresFolders: []string{path},
		Type:          sop.Standalone,
	}

	// 1. Test Setup (First run)
	savedOpts, err := database.Setup(ctx, opts)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}
	if len(savedOpts.StoresFolders) != 1 {
		t.Errorf("Expected 1 store folder, got %d", len(savedOpts.StoresFolders))
	}

	// 2. Test GetOptions
	loadedOpts, err := database.GetOptions(ctx, path)
	if err != nil {
		t.Fatalf("GetOptions failed: %v", err)
	}
	if loadedOpts.Type != opts.Type {
		t.Errorf("Expected Type %v, got %v", opts.Type, loadedOpts.Type)
	}

	// 3. Test Setup (Second run - should return error because it's already in memory)
	opts2 := sop.DatabaseOptions{
		StoresFolders: []string{path},
		Type:          sop.Clustered,
	}
	_, err = database.Setup(ctx, opts2)
	if err == nil {
		t.Error("Expected Setup (2nd run) to fail with 'already setup', got nil")
	}
}

func TestDatabase_Setup_ExistingOnDisk(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	// Manually write dboptions.json
	opts := sop.DatabaseOptions{
		StoresFolders: []string{path},
		Type:          sop.Clustered,
	}
	b, _ := json.Marshal(opts)
	if err := os.WriteFile(filepath.Join(path, "dboptions.json"), b, 0644); err != nil {
		t.Fatalf("Failed to write options file: %v", err)
	}

	// Call Setup - should detect existing file and return it
	// Note: We pass different options (Standalone) to verify it returns the one from disk (Clustered)
	inputOpts := sop.DatabaseOptions{
		StoresFolders: []string{path},
		Type:          sop.Standalone,
	}

	loadedOpts, err := database.Setup(ctx, inputOpts)
	if err != nil {
		t.Fatalf("Setup failed on existing file: %v", err)
	}
	if loadedOpts.Type != sop.Clustered {
		t.Errorf("Expected Setup to return existing options (Clustered), got %v", loadedOpts.Type)
	}
}

func TestDatabase_Setup_MultipleFolders(t *testing.T) {
	ctx := context.Background()
	path1 := t.TempDir()
	path2 := t.TempDir()

	opts := sop.DatabaseOptions{
		StoresFolders: []string{path1, path2},
		Type:          sop.Standalone,
	}

	_, err := database.Setup(ctx, opts)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Verify file exists in both folders
	for _, p := range []string{path1, path2} {
		if _, err := database.GetOptions(ctx, p); err != nil {
			t.Errorf("GetOptions failed for path %s: %v", p, err)
		}
	}
}

func TestDatabase_Setup_Errors(t *testing.T) {
	ctx := context.Background()

	// Empty StoresFolders
	opts := sop.DatabaseOptions{
		StoresFolders: []string{},
	}
	_, err := database.Setup(ctx, opts)
	if err == nil {
		t.Error("Expected error for empty StoresFolders, got nil")
	}
}
