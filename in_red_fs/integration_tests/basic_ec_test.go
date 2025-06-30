package integration_tests

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/fs"
	"github.com/SharedCode/sop/in_red_fs"
)

func initErasureCoding() {
	// Erasure Coding configuration lookup table (map).
	ec := make(map[string]fs.ErasureCodingConfig)

	// Erasure Coding config for "barstoreec" table uses three base folder paths that mimicks three disks.
	// Two data shards and one parity shard.
	ec["barstoreec"] = fs.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			fmt.Sprintf("%s%cdisk1", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk2", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk3", dataPath, os.PathSeparator),
		},
		RepairCorruptedShards: true,
	}
	// Erasure Coding config for default. Any B-tree that does not have a matching key in the EC config, will be given
	// a place in the default(key = "") eonfig entry.
	ec[""] = fs.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 2,
		BaseFolderPathsAcrossDrives: []string{
			fmt.Sprintf("%s%cdisk4", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk5", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk6", dataPath, os.PathSeparator),
			fmt.Sprintf("%s%cdisk7", dataPath, os.PathSeparator),
		},
		RepairCorruptedShards: true,
	}

	fs.SetGlobalErasureConfig(ec)
}

var storesFolders = []string{
	fmt.Sprintf("%s%cdisk1", dataPath, os.PathSeparator),
	fmt.Sprintf("%s%cdisk2", dataPath, os.PathSeparator),
}
var storesFoldersDefault = []string{
	fmt.Sprintf("%s%cdisk4", dataPath, os.PathSeparator),
	fmt.Sprintf("%s%cdisk5", dataPath, os.PathSeparator),
}

func Test_Basic_EC(t *testing.T) {
	ctx := context.Background()
	to, _ := in_red_fs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)
	trans, err := in_red_fs.NewTransactionWithReplication(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_fs.NewBtreeWithReplication[int, string](ctx, sop.StoreOptions{
		Name:                     "barstoreec",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
	}, trans, cmp.Compare)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if ok, err := b3.Find(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if k := b3.GetCurrentKey().Key; k != 1 {
		t.Errorf("GetCurrentKey() failed, got = %v, want = 1.", k)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		return
	}
	fmt.Println("Successfully added & found item with key 1.")
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_Basic_EC_Get(t *testing.T) {
	ctx := context.Background()
	to, _ := in_red_fs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)
	trans, err := in_red_fs.NewTransactionWithReplication(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_fs.NewBtreeWithReplication[int, string](ctx, sop.StoreOptions{
		Name:                     "barstoreec",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
	}, trans, cmp.Compare)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Find(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if k := b3.GetCurrentKey().Key; k != 1 {
		t.Errorf("GetCurrentKey() failed, got = %v, want = 1.", k)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		return
	}
	t.Logf("Successfully added & found item with key 1.")
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}
