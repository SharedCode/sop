//go:build stress
// +build stress

package replication

import (
	"context"
	"fmt"
	log "log/slog"
	"os"

	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
	"github.com/sharedcode/sop/inredfs/integrationtests/replication"
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
		Level: log.LevelDebug,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo

	inredfs.Initialize(redisConfig)

	// cache := redis.NewClient()
	// log.Info("about to issue cache.Clear")
	// ctx := context.Background()
	// if err := cache.Clear(ctx); err != nil {
	// 	log.Error(fmt.Sprintf("cache.Clear failed, details: %v", err))
	// }
	initErasureCoding()
	fs.DirectIOSim = replication.NewDirectIOReplicationSim(1)
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

func TestDirectIOSetupNewFileFailure_NoReplication(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "norepltable",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}, trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	// Failure due to DirectIO sim will throw on open file and cause rollback & error from trans commit.
	b3.Add(ctx, 1, "hello world")
	if err := trans.Commit(ctx); err == nil {
		t.Error("expected error but none was returned")
		t.FailNow()
	}
}

func TestDirectIOSetupNewFileFailure_WithReplication(t *testing.T) {
	fs.DirectIOSim = replication.NewDirectIOReplicationSim(1)
	ctx := context.Background()
	// Take from global EC config the data paths & EC config details.
	to, _ := inredfs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)

	trans, err := inredfs.NewTransactionWithReplication(ctx, to)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	trans.Begin()
	so := sop.StoreOptions{
		Name:                     "repltable",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}
	b3, err := inredfs.NewBtreeWithReplication[int, string](ctx, so, trans, nil)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	fmt.Printf("GlobalReplication ActiveFolderToggler Before Fail: %v\n", fs.GlobalReplicationDetails.ActiveFolderToggler)

	b3.Add(ctx, 1, "hello world")
	if err := trans.Commit(ctx); err == nil {
		t.Error("expected error but none was returned")
		t.FailNow()
	}

	fmt.Printf("GlobalReplication ActiveFolderToggler After Fail: %v\n", fs.GlobalReplicationDetails.ActiveFolderToggler)

	// Now, check whether transaction IO on new "active" target paths will be successful.
	fs.DirectIOSim = nil

	ctx = context.Background()
	trans, err = inredfs.NewTransactionWithReplication(ctx, to)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if err = trans.Begin(); err != nil {
		t.Error(err)
		t.FailNow()
	}
	b3, err = inredfs.NewBtreeWithReplication[int, string](ctx, so, trans, nil)
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

func TestOpenBtree_TransWithRepl_failed(t *testing.T) {
	fs.DirectIOSim = replication.NewDirectIOReplicationSim(1)

	ctx := context.Background()
	// Take from global EC config the data paths & EC config details.
	to, _ := inredfs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)

	trans, err := inredfs.NewTransactionWithReplication(ctx, to)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	trans.Begin()
	_, err = inredfs.OpenBtree[int, string](ctx, "repltable", trans, nil)
	if err == nil {
		t.Error("expected to fail but succeeded")
		t.FailNow()
	}
}

func TestOpenBtreeWithRepl_succeeded(t *testing.T) {
	fs.DirectIOSim = replication.NewDirectIOReplicationSim(1)

	ctx := context.Background()
	// Take from global EC config the data paths & EC config details.
	to, _ := inredfs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)

	trans, err := inredfs.NewTransactionWithReplication(ctx, to)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	trans.Begin()
	_, err = inredfs.OpenBtreeWithReplication[int, string](ctx, "repltable", trans, nil)
	if err != nil {
		t.Errorf("expected to succeed but failed, details: %v", err)
		t.FailNow()
	}
}
