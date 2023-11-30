package in_aws

import (
	"fmt"
	"testing"
)

func Test_TransactionStory_SingleBTree(t *testing.T) {
	t.Logf("Transaction story test.\n")
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	/* Sample code for a transaction and BTree:
	var trans Transaction
	trans.Begin()
	b3 := NewBtree(<..>, trans)
	b3.Add(..)
	b3.FindOne(..)
	..
	..
	trans.Commit()
	*/
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
