package in_red_ck

import (
	"testing"
)

func Test_OpenVsNewBTree(t *testing.T) {
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

func Test_SingleBTree(t *testing.T) {
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

// This test exercise & demonstrate to use B-Tree that is unique on Keys.
// Adding an item with a key matching an existing item in the trie will fail.
func Test_UniqueKeyBTree(t *testing.T) {
	trans, _ := newMockTransaction(t, true, -1)
	trans.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooWorld", 8, true, false, true, "", trans)
	b3.Add(ctx, 1, "hello world")
	b3.Add(ctx, 2, "foo bar")

	if ok, _ := b3.Add(ctx, 1, "this one will fail"); ok {
		t.Errorf("Add(1) failed, got true, want false, as key 1 exists.")
	}

	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

func Test_UniqueKeyBTreeAcrossCommits(t *testing.T) {
	t1, _ := newMockTransaction(t, true, -1)
	t1.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooWorld2", 8, true, false, true, "", t1)
	b3.Add(ctx, 1, "hello world")
	b3.Add(ctx, 2, "foo bar")

	if err := t1.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}

	t2, _ := newMockTransaction(t, true, -1)
	t2.Begin()
	// Open the same trie created above.
	b32, _ := OpenBtree[int, string](ctx, "fooWorld2", t2)
	if ok, _ := b32.Add(ctx, 1, "hello world"); ok {
		t.Errorf("Add(1) failed, got true, want false, as key 1 exists.")
	}

	if err := t2.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

// Fail 2nd commit as item key 1 was added in 1st and is also being added in 2nd.
func Test_UniqueKeyBTreeOnMultipleCommits(t *testing.T) {
	t1, _ := newMockTransaction(t, true, -1)
	t1.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooWorld3", 8, true, false, true, "", t1)
	b3.Add(ctx, 1, "hello world")
	b3.Add(ctx, 2, "foo bar")

	t2, _ := newMockTransaction(t, true, -1)
	t2.Begin()
	// Open the same trie created above.
	b32, _ := OpenBtree[int, string](ctx, "fooWorld3", t2)
	b32.Add(ctx, 1, "hello world")

	if err := t1.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
	if err := t2.Commit(ctx); err == nil {
		t.Errorf("Commit got nil, want error.")
	} else {
		t.Log(err)
	}
}
