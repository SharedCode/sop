package in_red_ck

import (
	"testing"
)

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	trans, _ := newMockTransaction(t, true, -1)
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
	trans, _ := newMockTransaction(t, true, -1)
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
