package value_data_segment

import (
	"bytes"
	"context"
	"testing"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/redis"
)

var cassConfig = cassandra.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

func init() {
	in_red_cfs.Initialize(cassConfig, redisConfig)
}

var ctx = context.Background()

const dataPath string = "/Users/grecinto/sop_data"

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "fooStore1",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
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
	if _, err := in_red_cfs.OpenBtree[int, string](ctx, "fooStore22", trans); err == nil {
		t.Logf("OpenBtree('fooStore', trans) failed, got nil want error.")
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
		Name:                     "fooStore2",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
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

func Test_ByteArrayValue(t *testing.T) {
	in_red_cfs.RemoveBtree(ctx, "baStore")
	trans, err := in_red_cfs.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, []byte](ctx, sop.StoreOptions{
		Name:                    "baStore",
		SlotLength:              8,
		LeafLoadBalancing:       true,
		Description:             "",
		BlobStoreBaseFolderPath: dataPath,
	}, trans)
	if err != nil {
		t.Error(err)
		return
	}
	value := []byte("hello world")
	if ok, err := b3.Add(ctx, 1, value); !ok || err != nil {
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
	if v, err := b3.GetCurrentValue(ctx); !bytes.Equal(v, value) || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = %v, nil.", v, err, value)
		return
	}
	t.Logf("Successfully added & found item with key 1.")
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_ByteArrayValueGet(t *testing.T) {
	trans, err := in_red_cfs.NewTransaction(sop.NoCheck, -1, false)
	if err != nil {
		t.Fatal(err.Error())
	}
	trans.Begin()
	b3, err := in_red_cfs.NewBtree[int, []byte](ctx, sop.StoreOptions{
		Name:                    "baStore",
		SlotLength:              8,
		LeafLoadBalancing:       true,
		Description:             "",
		BlobStoreBaseFolderPath: dataPath,
	}, trans)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.FindOne(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	value := []byte("hello world")
	if k := b3.GetCurrentKey(); k != 1 {
		t.Errorf("GetCurrentKey() failed, got = %v, want = 1.", k)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); !bytes.Equal(v, value) || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = %v, nil.", v, err, value)
		return
	}
	trans.Commit(ctx)
}
