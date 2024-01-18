package in_red_ck

import (
	"context"
	"testing"

	"github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/kafka"
	"github.com/SharedCode/sop/in_red_ck/redis"
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
	Initialize(cassConfig, redisConfig)
	kafka.Initialize(kafka.DefaultConfig)
	// Don't want to fill the kafka queue, so, this is commented out.
	//EnableDeleteService(true)
}

var ctx = context.Background()

// TODO: more unit tests to follow. Unit tests are intentionally delayed to cover more grounds in
// implementation, as we still don't have enough developers. But before each release, e.g. alpha,
// beta, beta 2, production release, unit tests will get good coverage. See in-memory beta release,
// it has good coverage.
//
// Dev't is slightly tweaked tailored for one-man show or developer resources scarcity.
// a.k.a. - extreme RAD(Rapid Application Development). No choice actually, but if more developers
// come/volunteer then the approach will change.

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooStore", 8, false, false, true, "", trans)
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		return
	}
	if _, err := OpenBtree[int, string](ctx, "fooStore22", trans); err == nil {
		t.Logf("OpenBtree('fooStore', trans) failed, got nil want error.")
	}
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooStore", 8, false, false, true, "", trans)
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

// Add Test_ prefix if wanting to run.
func TransactionInducedErrorOnNew(t *testing.T) {
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	NewBtree[int, string](ctx, "fooStore", 99, false, false, true, "", trans)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back after an error on NewBtree")
	}
}
