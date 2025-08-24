//go:build stress
// +build stress

package integrationtests

import (
	"context"
	"fmt"
	log "log/slog"
	"os"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
	"github.com/sharedcode/sop/redis"
)

// Redis config.
var redisConfig = redis.Options{
	Address:  "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
}

func getDataPath() string {
	// Read the 'home' data folder from Env if available.
	s := os.Getenv("datapath")
	if s == "" {
		s = "/Users/grecinto/sop_data"
	}
	return s
}

var dataPath string = getDataPath()

var testDefaultCacheConfig sop.StoreCacheConfig

func init() {
	l := log.New(log.NewJSONHandler(os.Stdout, &log.HandlerOptions{
		Level: log.LevelInfo,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo

	inredfs.Initialize(redisConfig)

	// Initialize Erasure Coding (EC) for the EC tests.
	initErasureCoding()

	cache := redis.NewClient()
	log.Info("about to issue cache.Clear")
	ctx := context.Background()
	if err := cache.Clear(ctx); err != nil {
		log.Error(fmt.Sprintf("cache.Clear failed, details: %v", err))
	}

	testDefaultCacheConfig = sop.GetDefaulCacheConfig()
	// Node Cache Duration for these tests is 5 minutes.
	testDefaultCacheConfig.NodeCacheDuration = time.Duration(5 * time.Minute)
	sop.SetDefaultCacheConfig(testDefaultCacheConfig)
}

func Test_GetStoreList(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	stores, _ := trans.GetStores(ctx)

	log.Info(fmt.Sprintf("Store count: %d", len(stores)))
}

// Create an empty store on 1st run, add one item(max) on succeeding runs.
func Test_CreateEmptyStore(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()

	b3, err := inredfs.OpenBtree[int, string](ctx, "emptyStore", trans, nil)
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
	trans, _ = inredfs.NewTransaction(ctx, to)
	trans.Begin()

	b3, err = inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "emptyStore",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
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
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "barstore2",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
	}, trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if _, err := inredfs.OpenBtree[int, string](ctx, "barStore22", trans, nil); err == nil {
		t.Logf("OpenBtree('barStore', trans) failed, got nil want error.")
	}
}

func Test_TransactionStory_SingleBTree_Get(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForReading, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "barstore1",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
	}, trans, nil)

	defer trans.Rollback(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	if b3.Count() == 0 {
		return
	}
	if ok, err := b3.Find(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if k := b3.GetCurrentKey().Key; k != 1 {
		t.Errorf("GetCurrentKey() failed, got = %v, want = 1.", k)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		return
	}
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	ctx := context.Background()

	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	// Demo NewTransactionExt specifying custom "to file path" lambda function.
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "barstore1",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
	}, trans, nil)
	if err != nil {
		t.Error(err)
		trans.Rollback(ctx)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}

	if ok, err := b3.Find(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if k := b3.GetCurrentKey().Key; k != 1 {
		t.Errorf("GetCurrentKey() failed, got = %v, want = 1.", k)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		trans.Rollback(ctx)
		return
	}
	t.Logf("Successfully added & found item with key 1.")
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_RegistryZeroDurationCache(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	so := sop.StoreCacheConfig{}
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "regnotcached",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        true,
		CacheConfig:              &so,
	}, trans, nil)
	if err != nil {
		t.Error(err)
		trans.Rollback(ctx)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}

	if ok, err := b3.Find(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if k := b3.GetCurrentKey().Key; k != 1 {
		t.Errorf("GetCurrentKey() failed, got = %v, want = 1.", k)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		trans.Rollback(ctx)
		return
	}
	t.Logf("Successfully added & found item with key 1.")
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_StoreCaching(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecaching",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
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

func Test_StoreCachingTTL(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "storecachingttl",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
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

	if ok, err := b3.Find(ctx, 1, false); !ok || err != nil {
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

func Test_BtreeOpenedTwice(t *testing.T) {
	ctx := context.Background()
	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, err := inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := inredfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "opentwice",
		SlotLength:               8,
		IsUnique:                 true,
		IsValueDataInNodeSegment: true,
		CacheConfig:              sop.NewStoreCacheConfig(time.Duration(30*time.Minute), true),
	}, trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if _, err := b3.AddIfNotExist(ctx, 1, "hello world"); err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(err) = %v, want = nil.", err)
		return
	}
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}

	// Do the test, open the btree twice (2 instances!) then commit.
	trans, err = inredfs.NewTransaction(ctx, to)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	_, err = inredfs.OpenBtree[int, string](ctx, "opentwice", trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	_, err = inredfs.OpenBtree[int, string](ctx, "opentwice", trans, nil)
	if err == nil {
		t.Error("got nil, expected error")
		return
	}

	trans.Commit(ctx)
}
