package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

func Test_ItemActionTracker_Remove_ActivelyPersisted_QueuesForDeletion(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{
		Name:                         "iat_remove_active",
		SlotLength:                   8,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: true,
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("rm", "a", "m", "e", "ph")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tr.Remove(ctx, it); err != nil {
		t.Fatalf("Remove err: %v", err)
	}
	if len(tr.forDeletionItems) != 1 || tr.forDeletionItems[0] != it.ID {
		t.Fatalf("expected item queued for deletion, got %#v", tr.forDeletionItems)
	}
	if it.ValueNeedsFetch {
		t.Fatalf("expected ValueNeedsFetch=false")
	}
}

func Test_ItemActionTracker_Remove_AfterAdd_DropsFromTracked(t *testing.T) {
	ctx := context.Background()
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_remove_after_add", SlotLength: 8})
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("rm2", "b", "m", "e", "ph")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tr.Add(ctx, it); err != nil {
		t.Fatalf("Add err: %v", err)
	}
	if _, ok := tr.items[it.ID]; !ok {
		t.Fatalf("expected item tracked after Add")
	}
	if err := tr.Remove(ctx, it); err != nil {
		t.Fatalf("Remove err: %v", err)
	}
	if _, ok := tr.items[it.ID]; ok {
		t.Fatalf("expected item removed from tracking after remove of new item")
	}
}
