package integration_tests

import (
	"context"
	"testing"
	"time"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/redis"
)

var cassConfig = cas.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

const dataPath string = "/Users/grecinto/sop_data"

func init() {
	in_red_cfs.Initialize(cassConfig, redisConfig)
}

var ctx = context.Background()

// Create an empty store on 1st run, add one item(max) on succeeding runs.
func Test_CreateEmptyStore(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()

	b3, err := in_red_cfs.OpenBtree[int, string](ctx, "emptyStore", trans)
	if err == nil {
		if b3.Count() == 0 {
			if ok, err := b3.Add(ctx, 123, "foobar"); !ok || err != nil {
				t.Errorf("Failed, w/ error: %v", err)
				return
			}
		}
		trans.Commit(ctx)
		return
	}
	trans, _ = in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	trans.Begin()

	b3, err = in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "emptyStore",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
		Description:              "",
		BlobStoreBaseFolderPath:  dataPath,
	}, trans)
	if err != nil {
		t.Error(err)
		return
	}
	if b3.Count() != 0 {
		t.Error("b3 not empty, expected Count = 0")
	}
	trans.Commit(ctx)
}

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "barstore2",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
		Description:              "",
		BlobStoreBaseFolderPath:  dataPath,
	}, trans)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if _, err := in_red_cfs.OpenBtree[int, string](ctx, "barStore22", trans); err == nil {
		t.Logf("OpenBtree('barStore', trans) failed, got nil want error.")
	}
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "barstore1",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
		Description:              "",
		BlobStoreBaseFolderPath:  dataPath,
	}, trans)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if ok, err := b3.FindOne(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if k := b3.GetCurrentKey(); k != 1 {
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

func Test_StoreCaching(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecaching",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		Description:              "",
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(30*time.Minute), false),
	}, trans)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if ok, err := b3.FindOne(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if k := b3.GetCurrentKey(); k != 1 {
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

func Test_StoreCachingTTL(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecachingttl",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		Description:              "",
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(30*time.Minute), true),
	}, trans)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}

	if ok, err := b3.FindOne(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
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
