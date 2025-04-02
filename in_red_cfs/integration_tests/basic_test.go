package integration_tests

import (
	"context"
	"fmt"
	log "log/slog"
	"os"
	"testing"
	"time"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/redis"
)

// Cassandra config.
var cassConfig = cas.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
// Redis config.
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

const dataPath string = "/Users/grecinto/sop_data"

func init() {
	l := log.New(log.NewJSONHandler(os.Stdout, &log.HandlerOptions{
		Level: log.LevelInfo,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo

	in_red_cfs.Initialize(cassConfig, redisConfig)
}

var ctx = context.Background()

func Test_GetStoreList(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	stores, _ := trans.GetStores(ctx)

	log.Info(fmt.Sprintf( "Store count: %d", len(stores)))
}

// Create an empty store on 1st run, add one item(max) on succeeding runs.
func Test_CreateEmptyStore(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()

	b3, err := in_red_cfs.OpenBtree[int, string](ctx, "emptyStore", trans, nil)
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
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
		BlobStoreBaseFolderPath:  dataPath,
	}, trans, nil)
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
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "barstore2",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
		BlobStoreBaseFolderPath:  dataPath,
	}, trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if _, err := in_red_cfs.OpenBtree[int, string](ctx, "barStore22", trans, nil); err == nil {
		t.Logf("OpenBtree('barStore', trans) failed, got nil want error.")
	}
}

func Test_TransactionStory_SingleBTree_Get(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	trans, err := in_red_cfs.NewTransaction(sop.ForReading, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.OpenBtree[int, string](ctx, "barstore1", trans, nil)
	if err != nil {
		t.Error(err)
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
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "barstore1",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
		BlobStoreBaseFolderPath:  dataPath,
	}, trans, nil)
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
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecaching",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(30*time.Minute), false),
	}, trans, nil)
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
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecachingttl",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		BlobStoreBaseFolderPath:  dataPath,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(30*time.Minute), true),
	}, trans, nil)
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
