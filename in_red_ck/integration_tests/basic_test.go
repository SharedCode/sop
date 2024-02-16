package integration_tests

import (
	"context"
	"testing"

	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/kafka"
	"github.com/SharedCode/sop/in_red_ck/redis"
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

func init() {
	in_red_ck.Initialize(cassConfig, redisConfig)
	kafka.Initialize(kafka.DefaultConfig)
	// Don't want to fill the kafka queue, so, this is commented out.
	//EnableDeleteService(true)
}

var ctx = context.Background()

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	trans, err := in_red_ck.NewTransaction(true, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := in_red_ck.NewBtree[int, string](ctx, "barStore", 8, false, true, true, "", trans)
	if err != nil {
		t.Error(err)
		return
	}
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if _, err := in_red_ck.OpenBtree[int, string](ctx, "barStore22", trans); err == nil {
		t.Logf("OpenBtree('barStore', trans) failed, got nil want error.")
	}
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	trans, err := in_red_ck.NewTransaction(true, -1, false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, err := in_red_ck.NewBtree[int, string](ctx, "barStore", 8, false, true, true, "", trans)
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

func TestGetOne(t *testing.T) {

	tl := cas.NewTransactionLog()
	uuid, _, r, err := tl.GetOne(ctx)
	if uuid.IsNil() {

	}
	if r == nil {

	}
	if err == nil {

	}
}
