package in_cas_s3

import (
	"context"
	"testing"
)

var ctx = context.Background()

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	t.Logf("Transaction story test.\n")
	trans := NewTransaction(true, -1)
	trans.Begin()
	b3, _ := NewBtree[int, string]("fooStore", 8, false, false, trans)
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if _, err := OpenBtree[int, string]("fooStore", trans); err == nil {
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
	trans := NewTransaction(true, -1)
	trans.Begin()
	b3, _ := NewBtree[int, string]("fooStore", 8, false, false, trans)
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}

	if ok, err := b3.FindOne(ctx, 1, false); !ok || err != nil {
		t.Logf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if k, err := b3.GetCurrentKey(ctx); k != 1 || err != nil {
		t.Logf("GetCurrentKey() failed, got = %v, %v, want = 1, nil.", k, err)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Logf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		trans.Rollback(ctx)
		return
	}
	t.Logf("Successfully added & found item with key 1.")
	if err := trans.Commit(ctx); err != nil {
		t.Logf("Commit returned error, details: %v.", err)
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
