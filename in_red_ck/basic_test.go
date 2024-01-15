package in_red_ck

import (
	"context"
	"testing"

	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_ck/cassandra"
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

func Test_TransactionWithInducedErrorOnAdd(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 1
	b3t.Add(ctx, 1, "foo")
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnAddIfNotExist(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 2
	b3t.AddIfNotExist(ctx, 1, "foo")
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnUpdate(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 3
	b3t.Update(ctx, 1, "foo")
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnUpdateCurrentItem(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 4
	b3t.UpdateCurrentItem(ctx, "foo")
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnRemove(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 5
	b3t.Remove(ctx, 1)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnRemoveCurrentItem(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 6
	b3t.RemoveCurrentItem(ctx)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnFindOne(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 7
	b3t.FindOne(ctx, 1, false)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnFindOneWithId(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 8
	b3t.FindOneWithId(ctx, 1, btree.NewUUID())
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnGetCurrentValue(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 9
	b3t.GetCurrentValue(ctx)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnGetCurrentItem(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 10
	b3t.GetCurrentItem(ctx)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnFirst(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 11
	b3t.First(ctx)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnLast(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 12
	b3t.Last(ctx)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnNext(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 13
	b3t.Next(ctx)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}

func Test_TransactionWithInducedErrorOnPrevious(t *testing.T) {
	t2, _ := NewTransaction(true, -1)
	t2.Begin()

	var t3 interface{} = t2.GetPhasedTransaction()
	trans := t3.(*transaction)

	b3 := newBTreeWithInducedErrors[int, string]()
	b3t := newBtreeWithTransaction(trans, b3)
	b3.induceErrorOnMethod = 14
	b3t.Previous(ctx)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back.")
	}
}
