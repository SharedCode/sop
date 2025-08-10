//go:build integration
// +build integration

package replication

import (
	"context"
	"fmt"
	log "log/slog"
	"math/rand"
	"os"

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
	fs.DirectIOSim = NewDirectIOReplicationSim(0)
	tableName := "repltable2"
	setupBtreeWithOneItem(tableName, rand.Intn(50)+1, t)

	writeData(tableName, rand.Intn(50)+1, "foobar", t)
	fmt.Printf("No error here.\n")

	// Set sim to fail on WriteAt.
	fs.DirectIOSim = NewDirectIOReplicationSim(2)
	fmt.Printf("Error here!\n")
	writeData(tableName, rand.Intn(50)+1, "bar bar", t)
	fmt.Printf("End of error\n")

	// Reinstate drive should succeed to reinstate the (failed) drives back to replication.
	reinstateDrive(t)

	fmt.Printf("Failed over and read foll. item Values: %v\n", readData(tableName, t))

	fs.DirectIOSim = NewDirectIOReplicationSim(0)
	writeData(tableName, rand.Intn(50)+1, "hey hey", t)
	fmt.Printf("No error here.\n")

	fmt.Printf("Failed over and read foll. item Values: %v\n", readData(tableName, t))

	// Fail on reading.
	cache := redis.NewClient()
	ctx := context.Background()
	if err := cache.Clear(ctx); err != nil {
		log.Error(fmt.Sprintf("cache.Clear failed, details: %v", err))
	}

	// Set sim to fail on ReadAt.
	fs.DirectIOSim = NewDirectIOReplicationSim(3)
	fmt.Printf("Failed on read, 'should be nil, %v\n", readData(tableName, t))

}

func TestMain2(t *testing.T) {
	fs.DirectIOSim = NewDirectIOReplicationSim(0)
	tableName := "repltable3"
	setupBtreeWithOneItem(tableName, rand.Intn(50)+1, t)

	writeData(tableName, rand.Intn(50)+1, "foobar", t)
	fmt.Printf("No error here.\n")

	// Set sim to fail on WriteAt, no failover, just IO error.
	fs.DirectIOSim = NewDirectIOReplicationSim(22)
	fmt.Printf("Error here!\n")
	writeData(tableName, rand.Intn(50)+1, "bar bar", t)
	fmt.Printf("End of error\n")

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
	if err = trans.Begin(); err != nil {
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
	if err = trans.Begin(); err != nil {
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
	if err = trans.Begin(); err != nil {
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
