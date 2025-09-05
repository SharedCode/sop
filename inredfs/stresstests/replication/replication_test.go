//go:build stress
// +build stress

package replication

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand"
	"os"
	"path/filepath"

	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
	"github.com/sharedcode/sop/redis"
)

const dataPath string = "/Users/grecinto/sop_data"

// Redis config.
var redisConfig = redis.Options{
	Address:  "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
}

func init() {
	l := log.New(log.NewJSONHandler(os.Stdout, &log.HandlerOptions{
		Level: log.LevelInfo,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo
	inredfs.Initialize(redisConfig)
	initErasureCoding()
}

func initErasureCoding() {
	// Erasure Coding configuration lookup table (map).
	ec := make(map[string]fs.ErasureCodingConfig)

	// Erasure Coding config for "barstoreec" table uses three base folder paths that mimicks three disks.
	// Two data shards and one parity shard.
	ec[""] = fs.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			fmt.Sprintf("%s%cdisk8", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk9", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk10", dataPath, os.PathSeparator),
		},
		RepairCorruptedShards: true,
	}
	fs.SetGlobalErasureConfig(ec)
}

var storesFolders = []string{
	fmt.Sprintf("%s%cdisk8", dataPath, os.PathSeparator),
	fmt.Sprintf("%s%cdisk9", dataPath, os.PathSeparator),
}

// Test to issue Reinstate of failed drives. But only works if the replcation flag is true and the replication status
// FailedToReplicate = true.
func reinstateDrive(t *testing.T) {
	ctx := context.Background()
	if err := inredfs.ReinstateFailedDrives(ctx, storesFolders); err != nil {
		t.Error(err)
		t.FailNow()
	}
	if fs.GlobalReplicationDetails.ActiveFolderToggler {
		fmt.Printf("Active Folder is: %s\n", storesFolders[0])
		return
	}
	fmt.Printf("Active Folder is: %s\n", storesFolders[1])
}

var transOptions, _ = inredfs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)

func TestMain(t *testing.T) {
	tableName := "repltable2"
	setupBtreeWithOneItem(tableName, rand.Intn(50)+1, t)

	writeData(tableName, rand.Intn(50)+1, "foobar", t)
	fmt.Printf("No error here.\n")

	// Induce write failure on current active registry segment to trigger failover.
	makeActiveRegistryReadOnly(t, tableName)
	fmt.Printf("Error here! (simulated via permissions)\n")
	writeData(tableName, rand.Intn(50)+1, "bar bar", t)
	fmt.Printf("End of error\n")

	// Restore permissions so reinstate can proceed.
	restoreRegistryPermissions(t, tableName)
	// Also restore perms for other known tables that other stress suites may have toggled,
	// to avoid cross-test interference during reinstate (which scans all tables).
	restoreRegistryPermissions(t, "repltable")
	restoreRegistryPermissions(t, "repltable3")
	// Reinstate drive should succeed to reinstate the (failed) drives back to replication.
	reinstateDrive(t)

	fmt.Printf("Failed over and read foll. item Values: %v\n", readData(tableName, t))

	writeData(tableName, rand.Intn(50)+1, "hey hey", t)
	fmt.Printf("No error here.\n")

	fmt.Printf("Failed over and read foll. item Values: %v\n", readData(tableName, t))

	// Fail on reading.
	cache := redis.NewClient()
	ctx := context.Background()
	if err := cache.Clear(ctx); err != nil {
		log.Error(fmt.Sprintf("cache.Clear failed, details: %v", err))
	}

	// Optionally, simulate read issue by making registry temporarily unreadable, then restore.
	makeActiveRegistryUnreadable(t, tableName)
	_ = readData(tableName, t) // may return empty on error paths
	restoreRegistryPermissions(t, tableName)

}

func TestMain2(t *testing.T) {
	tableName := "repltable3"
	setupBtreeWithOneItem(tableName, rand.Intn(50)+1, t)

	writeData(tableName, rand.Intn(50)+1, "foobar", t)
	fmt.Printf("No error here.\n")

	// Induce write error (no failover classification in some paths).
	makeActiveRegistryReadOnly(t, tableName)
	fmt.Printf("Error here! (simulated via permissions)\n")
	writeData(tableName, rand.Intn(50)+1, "bar bar", t)
	fmt.Printf("End of error\n")
	restoreRegistryPermissions(t, tableName)

}

func setupBtreeWithOneItem(btreeName string, itemID int, t *testing.T) {
	// Take from global EC config the data paths & EC config details.
	ctx := context.Background()
	// Setup a good B-tree succeeding with adding one entry & committed.
	trans, err := inredfs.NewTransactionWithReplication(ctx, transOptions)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err = trans.Begin(ctx); err != nil {
		t.Error(err)
		t.FailNow()
	}
	so := sop.StoreOptions{
		Name:                     btreeName,
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}
	b3, err := inredfs.NewBtreeWithReplication[int, string](ctx, so, trans, nil)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	_, err = b3.Add(ctx, itemID, "hello world")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("expected no error but got: %v", err)
		t.FailNow()
	}
	fmt.Printf("GlobalReplication ActiveFolderToggler at End: %v\n", fs.GlobalReplicationDetails.ActiveFolderToggler)
}

func writeData(btreeName string, itemID int, msg string, t *testing.T) {
	ctx := context.Background()
	trans, err := inredfs.NewTransactionWithReplication(ctx, transOptions)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err = trans.Begin(ctx); err != nil {
		t.Error(err)
		t.FailNow()
	}

	b3, err := inredfs.OpenBtreeWithReplication[int, string](ctx, btreeName, trans, nil)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	_, err = b3.Add(ctx, itemID, msg)
	if err != nil {
		t.Errorf("expected no error but got: %v", err)
		t.FailNow()
	}
	if err := trans.Commit(ctx); err != nil {
		fmt.Printf("got error: %v\n", err)
	}
	fmt.Printf("GlobalReplication ActiveFolderToggler at End: %v\n", fs.GlobalReplicationDetails.ActiveFolderToggler)
}

func readData(btreeName string, t *testing.T) []sop.KeyValuePair[int, string] {
	ctx := context.Background()
	trans, err := inredfs.NewTransactionWithReplication(ctx, transOptions)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err = trans.Begin(ctx); err != nil {
		t.Error(err)
		t.FailNow()
	}

	b3, err := inredfs.OpenBtreeWithReplication[int, string](ctx, btreeName, trans, nil)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	result := make([]sop.KeyValuePair[int, string], 0)
	b3.First(ctx)
	for {
		itm, err := b3.GetCurrentItem(ctx)
		if err != nil {
			return result
		}
		o := sop.KeyValuePair[int, string]{
			Key:   itm.Key,
			Value: *itm.Value,
		}
		result = append(result, o)
		if ok, _ := b3.Next(ctx); !ok {
			break
		}
	}
	trans.Commit(ctx)
	return result
}

func TestDirectIOCloseFileFailure(t *testing.T) {
}

func TestStoreRepositoryCreateStoreFailure(t *testing.T) {
}
func TestStoreRepositoryRemoveStoreFailure(t *testing.T) {
}
func TestStoreRepositoryWriteFileFailure(t *testing.T) {
}
func TestStoreRepositoryReadFileFailure(t *testing.T) {
}

// Helpers to simulate IO failures by toggling permissions on registry segment files.
func activeBaseFolder() string {
	if fs.GlobalReplicationDetails != nil && fs.GlobalReplicationDetails.ActiveFolderToggler {
		return storesFolders[1]
	}
	return storesFolders[0]
}

func registrySegmentPath(base, table string) string {
	return filepath.Join(base, table, fmt.Sprintf("%s-1.reg", table))
}

func ensureTableDir(t *testing.T, base, table string) string {
	t.Helper()
	dir := filepath.Join(base, table)
	_ = os.MkdirAll(dir, 0o755)
	return dir
}

func makeActiveRegistryReadOnly(t *testing.T, table string) {
	t.Helper()
	base := activeBaseFolder()
	ensureTableDir(t, base, table)
	seg := registrySegmentPath(base, table)
	// Best-effort chmod; if file doesn't exist yet, directory perms will block creation.
	_ = os.Chmod(seg, 0o444)
	_ = os.Chmod(filepath.Dir(seg), 0o555)
}

func makeActiveRegistryUnreadable(t *testing.T, table string) {
	t.Helper()
	base := activeBaseFolder()
	ensureTableDir(t, base, table)
	seg := registrySegmentPath(base, table)
	_ = os.Chmod(seg, 0o000)
}

func restoreRegistryPermissions(t *testing.T, table string) {
	t.Helper()
	for _, b := range storesFolders {
		dir := ensureTableDir(t, b, table)
		_ = os.Chmod(dir, 0o755)
		seg := registrySegmentPath(b, table)
		_ = os.Chmod(seg, 0o644)
	}
}
