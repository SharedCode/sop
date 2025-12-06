package database_test

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"

	// Ensure Redis adapter is registered for Clustered tests
	_ "github.com/sharedcode/sop/adapters/redis"
)

func TestDatabase_Standalone_Simple(t *testing.T) {
	storagePath := "/tmp/sop_test_standalone_simple"
	_ = os.RemoveAll(storagePath)
	defer os.RemoveAll(storagePath)

	db := database.NewDatabase(database.DatabaseOptions{
		StoresFolders: []string{storagePath},
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	store, err := db.NewBtree(ctx, "test_store", tx)
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
	basePath := "/tmp/sop_test_standalone_repl"
	_ = os.RemoveAll(basePath)
	defer os.RemoveAll(basePath)

	folders := []string{
		basePath + "/node1",
		basePath + "/node2",
	}
	for _, f := range folders {
		os.MkdirAll(f, 0755)
	}

	ecConfig := map[string]sop.ErasureCodingConfig{
		"test_store": {
			DataShardsCount:   1,
			ParityShardsCount: 1,
		},
	}

	db := database.NewDatabase(database.DatabaseOptions{
		StoresFolders: folders,
		ErasureConfig: ecConfig,
	})

	ctx := context.Background()
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Note: Store name must match EC config key or be handled by default?
	// Usually EC config is per store.
	store, err := db.NewBtree(ctx, "test_store", tx)
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

	db := database.NewDatabase(database.DatabaseOptions{
		CacheType:     sop.Redis,
		StoresFolders: []string{"/tmp/sop_test_clustered"},
	})

	if db == nil {
		t.Fatal("NewDatabase returned nil for Clustered")
	}

	// We expect BeginTransaction to fail or panic if Redis is not reachable,
	// but we can try it to see what happens.
	ctx := context.Background()
	_, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err == nil {
		// If it succeeds (maybe mock redis?), great.
		// If it fails, we check if it's a connection error.
		t.Log("BeginTransaction succeeded (unexpected without Redis)")
	} else {
		t.Logf("BeginTransaction failed as expected (no Redis): %v", err)
	}
}

func TestDatabase_Cassandra_Construction(t *testing.T) {
	db := database.NewCassandraDatabase(database.DatabaseOptions{
		Keyspace:      "test_keyspace",
		StoresFolders: []string{"/tmp/sop_test_cassandra"},
	})

	if db == nil {
		t.Fatal("NewCassandraDatabase returned nil")
	}
}

func TestDatabase_Cassandra_Transaction_Simple(t *testing.T) {
	db := database.NewCassandraDatabase(database.DatabaseOptions{
		Keyspace:      "test_keyspace",
		StoresFolders: []string{"/tmp/sop_test_cassandra_simple"},
	})

	ctx := context.Background()
	// Expect error connecting to Cassandra
	_, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err == nil {
		t.Fatal("Expected error connecting to Cassandra, got nil")
	}
	t.Logf("Got expected error: %v", err)
}

func TestDatabase_Cassandra_Transaction_Replication(t *testing.T) {
	// Setup folders
	basePath := "/tmp/sop_test_cassandra_repl"
	_ = os.RemoveAll(basePath)
	defer os.RemoveAll(basePath)

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

	db := database.NewCassandraDatabase(database.DatabaseOptions{
		Keyspace:      "test_keyspace",
		ErasureConfig: ecConfig,
	})

	ctx := context.Background()
	_, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err == nil {
		t.Fatal("Expected error connecting to Cassandra, got nil")
	}
	t.Logf("Got expected error: %v", err)
}
