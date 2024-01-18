package in_red_ck

import (
	"testing"

	"github.com/SharedCode/sop"
)

func Test_TransactionInducedErrorOnOpen(t *testing.T) {
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	OpenBtree[int, string](ctx, "fooStore33", trans)
	if trans.HasBegun() {
		t.Error("Transaction is not rolled back after an error on OpenBtree")
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
	b3t.FindOneWithId(ctx, 1, sop.NewUUID())
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
