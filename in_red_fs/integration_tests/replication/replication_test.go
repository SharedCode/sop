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
		Level: log.LevelInfo,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo

	in_red_fs.Initialize(redisConfig)

	cache := redis.NewClient()
	log.Info("about to issue cache.Clear")
	ctx := context.Background()
	if err := cache.Clear(ctx); err != nil {
		log.Error(fmt.Sprintf("cache.Clear failed, details: %v", err))
	}
	initErasureCoding()
}

func initErasureCoding() {
	// Erasure Coding configuration lookup table (map).
	ec := make(map[string]fs.ErasureCodingConfig)

	// Erasure Coding config for "barstoreec" table uses three base folder paths that mimicks three disks.
	// Two data shards and one parity shard.
	ec["repltable"] = fs.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			fmt.Sprintf("%s%cdisk1", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk2", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk3", dataPath, os.PathSeparator),
		},
		RepairCorruptedShards: true,
	}
	fs.SetGlobalErasureConfig(ec)
}

func TestDirectIOSetupNewFileFailure_NoReplication(t *testing.T) {
	fs.DirectIOSim = newDirectIOReplicationSim()

	ctx := context.Background()
	to, _ := in_red_fs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := in_red_fs.NewTransaction(to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_fs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "repltable",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}, trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	b3.Add(ctx, 1, "hello world")
	if err := trans.Commit(ctx); err == nil {
		t.Error("expected error but none was returned")
		t.FailNow()
	}
}

func TestDirectIOSetupNewFileFailure_WithReplication(t *testing.T) {
	fs.DirectIOSim = newDirectIOReplicationSim()

	ctx := context.Background()
	// Take from global EC config the data paths & EC config details.
	to, _ := in_red_fs.NewTransactionOptionsWithReplication(nil, sop.ForWriting, -1, fs.MinimumModValue, nil)
	trans, err := in_red_fs.NewTransactionWithReplication(to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_fs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "repltable",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}, trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	b3.Add(ctx, 1, "hello world")
	if err := trans.Commit(ctx); err == nil {
		t.Error("expected error but none was returned")
		t.FailNow()
	}
}

func TestDirectIOReadFromFileFailure(t *testing.T) {
}
func TestDirectIOWriteToFileFailure(t *testing.T) {
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
