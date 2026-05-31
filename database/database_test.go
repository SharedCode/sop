package database_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/adapters/redis"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/database"
)

type commitToggleRedisCache struct {
	base       sop.L2Cache
	mu         sync.Mutex
	failCommit bool
}

func newCommitToggleRedisCache() *commitToggleRedisCache {
	return &commitToggleRedisCache{base: mocks.NewMockClient()}
}

func (m *commitToggleRedisCache) EnableCommitFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCommit = true
}

func (m *commitToggleRedisCache) shouldFailCommit() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failCommit
}

func (m *commitToggleRedisCache) GetType() sop.L2CacheType { return sop.Redis }
func (m *commitToggleRedisCache) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return m.base.Set(ctx, key, value, expiration)
}
func (m *commitToggleRedisCache) Get(ctx context.Context, key string) (bool, string, error) {
	return m.base.Get(ctx, key)
}
func (m *commitToggleRedisCache) GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error) {
	return m.base.GetEx(ctx, key, expiration)
}
func (m *commitToggleRedisCache) IsRestarted(ctx context.Context) bool {
	return m.base.IsRestarted(ctx)
}
func (m *commitToggleRedisCache) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return m.base.SetStruct(ctx, key, value, expiration)
}
func (m *commitToggleRedisCache) SetStructs(ctx context.Context, keys []string, values []interface{}, expiration time.Duration) error {
	return m.base.SetStructs(ctx, keys, values, expiration)
}
func (m *commitToggleRedisCache) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	return m.base.GetStruct(ctx, key, target)
}
func (m *commitToggleRedisCache) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error) {
	return m.base.GetStructEx(ctx, key, target, expiration)
}
func (m *commitToggleRedisCache) GetStructs(ctx context.Context, keys []string, targets []interface{}, expiration time.Duration) ([]bool, error) {
	return m.base.GetStructs(ctx, keys, targets, expiration)
}
func (m *commitToggleRedisCache) Delete(ctx context.Context, keys []string) (bool, error) {
	return m.base.Delete(ctx, keys)
}
func (m *commitToggleRedisCache) Ping(ctx context.Context) error { return nil }
func (m *commitToggleRedisCache) FormatLockKey(k string) string  { return m.base.FormatLockKey(k) }
func (m *commitToggleRedisCache) CreateLockKeys(keys []string) []*sop.LockKey {
	return m.base.CreateLockKeys(keys)
}
func (m *commitToggleRedisCache) CreateLockKeysForIDs(keys []sop.Tuple[string, sop.UUID]) []*sop.LockKey {
	return m.base.CreateLockKeysForIDs(keys)
}
func (m *commitToggleRedisCache) IsLockedTTL(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, error) {
	return m.base.IsLockedTTL(ctx, duration, lockKeys)
}
func (m *commitToggleRedisCache) Lock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	if m.shouldFailCommit() {
		return false, sop.UUID{}, fmt.Errorf("redis unavailable during commit")
	}
	return m.base.Lock(ctx, duration, lockKeys)
}
func (m *commitToggleRedisCache) DualLock(ctx context.Context, duration time.Duration, lockKeys []*sop.LockKey) (bool, sop.UUID, error) {
	if m.shouldFailCommit() {
		return false, sop.UUID{}, fmt.Errorf("redis unavailable during commit")
	}
	return m.base.DualLock(ctx, duration, lockKeys)
}
func (m *commitToggleRedisCache) IsLocked(ctx context.Context, lockKeys []*sop.LockKey) (bool, error) {
	return m.base.IsLocked(ctx, lockKeys)
}
func (m *commitToggleRedisCache) IsLockedByOthers(ctx context.Context, lockKeyNames []string) (bool, error) {
	return m.base.IsLockedByOthers(ctx, lockKeyNames)
}
func (m *commitToggleRedisCache) IsLockedByOthersTTL(ctx context.Context, lockKeyNames []string, duration time.Duration) (bool, error) {
	return m.base.IsLockedByOthersTTL(ctx, lockKeyNames, duration)
}
func (m *commitToggleRedisCache) Unlock(ctx context.Context, lockKeys []*sop.LockKey) error {
	return m.base.Unlock(ctx, lockKeys)
}
func (m *commitToggleRedisCache) Clear(ctx context.Context) error { return m.base.Clear(ctx) }

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

func TestCommit_ReturnsErrorWhenRedisBecomesUnavailable(t *testing.T) {
	uniqueAddr := fmt.Sprintf("commit-unreachable-%d:6379", time.Now().UnixNano())
	cache := newCommitToggleRedisCache()
	sop.RegisterL2CacheFactory(sop.Redis, func(opts sop.TransactionOptions) sop.L2Cache {
		return cache
	})
	t.Cleanup(func() {
		sop.RegisterL2CacheFactory(sop.Redis, redis.NewClient)
	})

	db, err := database.ValidateOptions(sop.DatabaseOptions{
		Type:          sop.Clustered,
		CacheType:     sop.Redis,
		StoresFolders: []string{t.TempDir()},
		RedisConfig: &sop.RedisCacheConfig{
			Address: uniqueAddr,
		},
	})
	if err != nil {
		t.Fatalf("ValidateOptions returned error: %v", err)
	}

	ctx := context.Background()
	tx, err := database.BeginTransaction(ctx, db, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed unexpectedly: %v", err)
	}

	store, err := database.NewBtree[string, string](ctx, db, "test_store", tx, nil)
	if err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}
	if _, err := store.Add(ctx, "key1", "value1"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	cache.EnableCommitFailure()
	err = tx.Commit(ctx)
	if err == nil {
		t.Fatal("expected Commit to return error when Redis fails during commit")
	}
	if got := err.Error(); got == "" || !containsAll(got, []string{"redis unavailable during commit"}) {
		t.Fatalf("expected commit redis error, got %v", err)
	}
}

func containsAll(s string, parts []string) bool {
	for _, part := range parts {
		if !strings.Contains(s, part) {
			return false
		}
	}
	return true
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

func TestDatabase_Setup_ManualDeletion(t *testing.T) {
	storagePath := t.TempDir()
	opts := sop.DatabaseOptions{
		StoresFolders: []string{storagePath},
	}

	ctx := context.Background()

	// 1. First Setup
	_, err := database.Setup(ctx, opts)
	if err != nil {
		t.Fatalf("First setup failed: %v", err)
	}

	// 2. Verify "Already Setup" error if we try again immediately
	_, err = database.Setup(ctx, opts)
	if err == nil {
		t.Fatalf("Expected error for second setup, got nil")
	}

	// 3. Manually delete the file (simulate external deletion)
	optionsFile := filepath.Join(storagePath, "dboptions.json")
	if err := os.Remove(optionsFile); err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}

	// 4. Try Setup again - Should succeed now (with fix)
	_, err = database.Setup(ctx, opts)
	if err != nil {
		t.Fatalf("Setup after manual deletion failed: %v", err)
	}
}

func TestDatabase_Remove_Replicated(t *testing.T) {
	// Setup folders for replication
	basePath := t.TempDir()
	folder1 := filepath.Join(basePath, "node1")
	folder2 := filepath.Join(basePath, "node2")

	opts := sop.DatabaseOptions{
		StoresFolders: []string{folder1, folder2},
		CacheType:     sop.InMemory,
	}

	ctx := context.Background()

	// 1. Setup
	if _, err := database.Setup(ctx, opts); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Verify folders exist
	if _, err := os.Stat(folder1); os.IsNotExist(err) {
		t.Errorf("Folder1 should exist after setup")
	}
	if _, err := os.Stat(folder2); os.IsNotExist(err) {
		t.Errorf("Folder2 should exist after setup")
	}

	// 2. Remove
	// We pass folder1 as the "dbPath" (primary)
	if err := database.Remove(ctx, folder1); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// 3. Verify BOTH are gone
	if _, err := os.Stat(folder1); !os.IsNotExist(err) {
		t.Errorf("Folder1 should be deleted")
	}
	if _, err := os.Stat(folder2); !os.IsNotExist(err) {
		t.Errorf("Folder2 should be deleted")
	}
}
