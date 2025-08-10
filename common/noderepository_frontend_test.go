package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// Covers the thin frontend wrappers Add/Update/Get/Fetched/Remove which were previously untested.
func Test_NodeRepository_Frontend_Wrappers(t *testing.T) {
	ctx := context.Background()
	// Wire a transaction with mocks and global L1 cache bound to mock L2.
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	reg := mocks.NewMockRegistry(false)
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: l2, blobStore: blobs, l1Cache: cache.GetGlobalCache()}

	so := sop.StoreOptions{Name: "nr_front", SlotLength: 4, IsValueDataInNodeSegment: true}
	si := sop.NewStoreInfo(so)
	nr := newNodeRepository[PersonKey, Person](tx, si)

	// Prepare a node and exercise Add/Update/Remove wrappers
	n := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
	nr.Add(n)
	if v, ok := nr.localCache[n.ID]; !ok || v.action != addAction {
		t.Fatalf("Add did not mark local cache addAction; ok=%v action=%v", ok, v.action)
	}

	// Update should keep addAction when previously added
	n.Version = 2
	nr.Update(n)
	if v := nr.localCache[n.ID]; v.action != addAction {
		t.Fatalf("Update should keep addAction for new node; got %v", v.action)
	}

	// Fetched moves read MRU entry into local cache
	nr.readNodesCache.Set([]sop.KeyValuePair[sop.UUID, any]{{Key: n.ID, Value: n}})
	nr.Fetched(n.ID)
	if v, ok := nr.localCache[n.ID]; !ok || v.action != getAction || v.node == nil {
		t.Fatalf("Fetched did not migrate node to local cache properly; ok=%v action=%v", ok, v.action)
	}

	// Remove should mark removeAction when present
	nr.Remove(n.ID)
	if v := nr.localCache[n.ID]; v.action != removeAction {
		t.Fatalf("Remove did not set removeAction; got %v", v.action)
	}

	// Get should read using registry + blob path when not in local/MRU.
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.Version = 3
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	// Seed blob for active ID in handle
	nb := &btree.Node[PersonKey, Person]{ID: h.GetActiveID(), Version: h.Version}
	if err := blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: si.BlobTable, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: nb.ID, Value: mustNode(nb)}}}}); err != nil {
		t.Fatalf("seed blob: %v", err)
	}
	// Now Get via frontend should hydrate from blob and cache in MRU/reads
	got, err := nr.Get(ctx, lid)
	if err != nil || got == nil || got.ID != nb.ID || got.Version != h.Version {
		t.Fatalf("Get did not return expected node; err=%v got=%+v", err, got)
	}
	// Ensure MRU was populated in L1 cache deterministically (avoid relying on global L2 instance)
	var out btree.Node[PersonKey, Person]
	if nr.l1Cache.GetNodeFromMRU(h, &out) == nil || out.ID != nb.ID || out.Version != h.Version {
		t.Fatalf("expected node cached in L1 MRU via SetNode; got ID=%v ver=%d", out.ID, out.Version)
	}

	_, _ = l2.Delete(ctx, []string{cache.FormatNodeKey(nb.ID.String())})
	_, _ = l2.Delete(ctx, []string{l2.FormatLockKey(lid.String())})
	_ = l2.Set(ctx, l2.FormatLockKey("cleanup"), sop.NewUUID().String(), time.Second)
}

// Helper to marshal a node using the same marshaler as production cache.
func mustNode(n *btree.Node[PersonKey, Person]) []byte {
	b, _ := encoding.BlobMarshaler.Marshal(n)
	return b
}
