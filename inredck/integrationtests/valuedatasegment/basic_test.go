package valuedatasegment

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cassandra"
	"github.com/sharedcode/sop/inredck"
	"github.com/sharedcode/sop/redis"
)

var cassConfig = cassandra.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options{
	Address:  "localhost:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
}

func init() {
	inredck.Initialize(cassConfig, redisConfig)
}

var ctx = context.Background()

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	trans, err := inredck.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := inredck.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "fooStore",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        true,
		Description:              "",
	}, trans, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if _, err := inredck.OpenBtree[int, string](ctx, "fooStore22", trans, nil); err == nil {
		t.Logf("OpenBtree('fooStore', trans) failed, got nil want error.")
	}
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	trans, err := inredck.NewTransaction(sop.ForWriting, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := inredck.NewBtree[int, string](ctx, sop.StoreOptions{
		Name:                     "fooStore",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        true,
		Description:              "",
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
