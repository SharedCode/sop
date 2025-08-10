package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

// helper to quickly create a writer transaction and a new btree with provided options
func mustNewBtree[TK btree.Ordered, TV any](t *testing.T, si sop.StoreOptions, comparer btree.ComparerFunc[TK]) (sop.Transaction, btree.BtreeInterface[TK, TV]) {
	t.Helper()
	trans, err := newMockTransaction(t, sop.ForWriting, -1)
	if err != nil {
		t.Fatalf("newMockTransaction error: %v", err)
	}
	trans.Begin()
	b3, err := NewBtree[TK, TV](context.Background(), si, trans, comparer)
	if err != nil {
		t.Fatalf("NewBtree error: %v", err)
	}
	return trans, b3
}

// Seeds a store with a single item and commits the transaction.
func seedStoreWithOne(t *testing.T, name string, inNode bool, key PersonKey, value Person) {
	t.Helper()
	si := sop.StoreOptions{
		Name:                     name,
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: inNode,
		LeafLoadBalancing:        false,
	}
	trans, b3 := mustNewBtree[PersonKey, Person](t, si, Compare)
	ok, err := b3.Add(context.Background(), key, value)
	if !ok || err != nil {
		t.Fatalf("seed Add failed: ok=%v err=%v", ok, err)
	}
	trans.Commit(context.Background())
}

func Test_AreFetchedItemsIntact_Cases(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg}}

	// Case 1: empty input
	if ok, err := nr.areFetchedItemsIntact(ctx, nil); err != nil || !ok {
		t.Fatalf("expected ok for empty input, got ok=%v err=%v", ok, err)
	}

	// Build a single node tuple with version 1
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "afi", SlotLength: 8, IsValueDataInNodeSegment: true})
	n := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
	nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{
		{First: si, Second: []interface{}{n}},
	}
	// Seed registry with matching handle/version -> ok
	h := sop.NewHandle(n.ID)
	h.Version = 1
	reg.(*mocks.Mock_vid_registry).Lookup[n.ID] = h
	if ok, err := nr.areFetchedItemsIntact(ctx, nodes); err != nil || !ok {
		t.Fatalf("expected ok when versions match, got ok=%v err=%v", ok, err)
	}

	// Change version in registry -> should detect change
	h.Version = 2
	reg.(*mocks.Mock_vid_registry).Lookup[n.ID] = h
	if ok, err := nr.areFetchedItemsIntact(ctx, nodes); err != nil || ok {
		t.Fatalf("expected not ok on version change, got ok=%v err=%v", ok, err)
	}
}

func Test_RefetchAndMerge_Paths(t *testing.T) {
	ctx := context.Background()
	type tc struct {
		name   string
		inNode bool
		action string
	}
	cases := []tc{
		{name: "add_out_of_node", inNode: false, action: "add"},
		{name: "add_in_node", inNode: true, action: "add"},
		{name: "get_existing", inNode: true, action: "get"},
		{name: "remove_existing", inNode: true, action: "remove"},
		{name: "update_in_node", inNode: true, action: "update"},
		{name: "update_out_of_node", inNode: false, action: "update"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			storeName := "rfm_" + c.name
			pk, p := newPerson("a", storeName, "x", "e@x", "p")

			// For existing-item actions, seed a committed store first.
			if c.action != "add" {
				seedStoreWithOne(t, storeName, c.inNode, pk, p)
			}

			si := sop.StoreOptions{
				Name:                     storeName,
				SlotLength:               8,
				IsUnique:                 false,
				IsValueDataInNodeSegment: c.inNode,
				LeafLoadBalancing:        false,
			}
			trans, b3 := mustNewBtree[PersonKey, Person](t, si, Compare)

			switch c.action {
			case "add":
				ok, err := b3.Add(ctx, pk, p)
				if !ok || err != nil {
					t.Fatalf("Add failed: ok=%v err=%v", ok, err)
				}
			case "get":
				ok, err := b3.Find(ctx, pk, false)
				if !ok || err != nil {
					t.Fatalf("Find failed: ok=%v err=%v", ok, err)
				}
				if _, err := b3.GetCurrentValue(ctx); err != nil {
					t.Fatalf("GetCurrentValue failed: %v", err)
				}
			case "remove":
				if _, err := b3.Remove(ctx, pk); err != nil {
					t.Fatalf("Remove failed: %v", err)
				}
			case "update":
				p2 := p
				p2.Email = "changed@x"
				if _, err := b3.Update(ctx, pk, p2); err != nil {
					t.Fatalf("Update failed: %v", err)
				}
			}

			// Call the refetch-and-merge closure through the underlying transaction.
			t2 := trans.GetPhasedTransaction().(*Transaction)
			if err := t2.refetchAndMergeModifications(ctx); err != nil {
				t.Fatalf("refetchAndMergeModifications returned error: %v", err)
			}
			trans.Rollback(ctx) // end this transaction cleanly
		})
	}
}

func Test_RefetchAndMerge_Conflict_VersionMismatch(t *testing.T) {
	ctx := context.Background()
	storeName := "rfm_conflict"
	pk, p := newPerson("bob", "conflict", "m", "b@x", "1")

	// Seed one committed item.
	seedStoreWithOne(t, storeName, true, pk, p)

	// Start T1 (reader/writer doesn't matter for calling the helper), read item to record version.
	si := sop.StoreOptions{Name: storeName, SlotLength: 8, IsValueDataInNodeSegment: true}
	t1, b3 := mustNewBtree[PersonKey, Person](t, si, Compare)
	ok, err := b3.Find(ctx, pk, false)
	if !ok || err != nil {
		t.Fatalf("T1 Find failed: ok=%v err=%v", ok, err)
	}
	if _, err := b3.GetCurrentValue(ctx); err != nil {
		t.Fatalf("T1 GetCurrentValue failed: %v", err)
	}

	// Start T2: update same item and commit to bump version in backend.
	t2, b3w := mustNewBtree[PersonKey, Person](t, si, Compare)
	p2 := p
	p2.Email = "new@x"
	if _, err := b3w.Update(ctx, pk, p2); err != nil {
		t.Fatalf("T2 Update failed: %v", err)
	}
	t2.Commit(ctx)

	// Now T1 refetch-and-merge should detect newer version and return error.
	if err := t1.GetPhasedTransaction().(*Transaction).refetchAndMergeModifications(ctx); err == nil {
		t.Fatalf("expected version conflict error, got nil")
	}
	t1.Rollback(ctx)
}
