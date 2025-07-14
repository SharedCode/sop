package replication

import (
	"context"
	"fmt"
	log "log/slog"
	"os"

	"testing"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/fs"
	"github.com/SharedCode/sop/in_red_fs"
	"github.com/SharedCode/sop/redis"
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
		Level: log.LevelDebug,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo
	in_red_fs.Initialize(redisConfig)
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
	if err := in_red_fs.ReinstateFailedDrives(ctx, storesFolders, nil, fs.MinimumModValue); err != nil {
		t.Error(err)
		t.FailNow()
	}
}

var transOptions, _ = in_red_fs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)

func TestMain(t *testing.T) {
	fs.DirectIOSim = NewDirectIOReplicationSim(0)

	setupBtreeWithOneItem("repltable2", t)

	// Generate a synthetic failure on Write causing fallback event.
	fs.DirectIOSim = NewDirectIOReplicationSim(2)
	failOnWrite("repltable2", t)

	// Reinstate drive should succeed to flip active & passive.
	reinstateDrive(t)
	
}

func setupBtreeWithOneItem(name string, t *testing.T) {
	// Take from global EC config the data paths & EC config details.
	ctx := context.Background()
	// Setup a good B-tree succeeding with adding one entry & committed.
	trans, err := in_red_fs.NewTransactionWithReplication(ctx, transOptions)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err = trans.Begin(); err != nil {
		t.Error(err)
		t.FailNow()
	}
	so := sop.StoreOptions{
		Name:                     name,
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}
	b3, err := in_red_fs.NewBtreeWithReplication[int, string](ctx, so, trans, nil)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	_, err = b3.Add(ctx, 1, "hello world")
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

func failOnWrite(name string, t *testing.T) {
	ctx := context.Background()
	trans, err := in_red_fs.NewTransactionWithReplication(ctx, transOptions)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err = trans.Begin(); err != nil {
		t.Error(err)
		t.FailNow()
	}

	b3, err := in_red_fs.OpenBtreeWithReplication[int, string](ctx, name, trans, nil)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	_, err = b3.Add(ctx, 2, "hello world")
	if err != nil {
		t.Errorf("expected no error but got: %v", err)
		t.FailNow()
	}
	if err := trans.Commit(ctx); err == nil {
		t.Errorf("got nil, expected error")
		t.FailNow()
	}
	fmt.Printf("GlobalReplication ActiveFolderToggler at End: %v\n", fs.GlobalReplicationDetails.ActiveFolderToggler)
}

func TestDirectIOReadFromFileFailure(t *testing.T) {

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
