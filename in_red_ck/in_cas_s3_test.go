package in_red_ck

import (
	"context"
	"testing"

	"github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

var cassConfig = cassandra.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options {
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
	t.Logf("Transaction story test.\n")
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooStore", 8, false, false, true, "", trans)
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if _, err := OpenBtree[int, string](ctx, "fooStore", trans); err == nil {
		t.Logf("OpenBtree('fooStore', trans) failed, got nil want error.")
		trans.Rollback(ctx)
	}
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	t.Logf("Transaction story test.\n")
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
		trans.Rollback(ctx)
		return
	}

	if ok, err := b3.FindOne(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if k, err := b3.GetCurrentKey(ctx); k != 1 || err != nil {
		t.Errorf("GetCurrentKey() failed, got = %v, %v, want = 1, nil.", k, err)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		trans.Rollback(ctx)
		return
	}
	t.Logf("Successfully added & found item with key 1.")
	// TODO: add more unit tests to exercise/verify commit's conflict detection, lightweight locking
	// and Nodes upserts, i.e. - save updated nodes, save removed nodes & save added nodes.
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_TransactionStory_ManyBTree(t *testing.T) {
	t.Logf("Transaction story test.\n")
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Instantiate a BTree2
	// 3. Do CRUD on BTree & BTree2
	// 4. Commit Transaction
}

func Test_TransactionManagerStory(t *testing.T) {
	t.Logf("Transaction Manager story test.\n")
	// 1. Instantiate transaction manager
	// 2. All BTrees should now be "transactional" implicitly, i.e. - BTree will create/commit
	//    transaction if there is not one, or explicitly, i.e. - user invoked begin/commit transaction.
	// 3. All BTrees should be registered/accounted for in the active Transaction where they got instantiated/CRUDs.
	//    - On Commit, transaction will persist all changes in all BTrees it accounted for.
	//    - On Rollback, transaction will undo or not save the canged done in all BTree it accounted for.
	//
}