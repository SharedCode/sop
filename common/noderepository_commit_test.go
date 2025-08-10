package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

func Test_NodeRepository_CommitAddedNodes(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}
	nr := &nodeRepositoryBackend{transaction: tx}

	so := sop.StoreOptions{Name: "st_add", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	n := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}

	handles, err := nr.commitAddedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("commitAddedNodes error: %v", err)
	}
	if len(handles) != 1 || len(handles[0].IDs) != 1 {
		t.Fatalf("unexpected handles: %+v", handles)
	}
	// Blob persisted
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, n.ID); len(ba) == 0 {
		t.Fatalf("blob not persisted for added node")
	}
	// Cache persisted
	var out btree.Node[PersonKey, Person]
	if ok, _ := redis.GetStruct(ctx, nr.formatKey(n.ID.String()), &out); !ok {
		t.Fatalf("redis cache not set for added node")
	}
	// Registry has the handle with incremented version (NewHandle starts at 0; commitAddedNodes bumps once to 1)
	got, _ := reg.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, IDs: []sop.UUID{n.ID}}})
	if len(got) != 1 || len(got[0].IDs) != 1 || got[0].IDs[0].Version != 1 {
		t.Fatalf("registry handle not added or wrong version: %+v", got)
	}
}

func Test_NodeRepository_CommitUpdatedNodes_Succeeds(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}
	nr := &nodeRepositoryBackend{transaction: tx}

	so := sop.StoreOptions{Name: "st_upd", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 3}
	// Seed registry with matching handle and version
	h := sop.NewHandle(lid)
	h.Version = 3
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}

	ok, handles, err := nr.commitUpdatedNodes(ctx, nodes)
	if err != nil || !ok {
		t.Fatalf("commitUpdatedNodes failed: ok=%v err=%v", ok, err)
	}
	if len(handles) != 1 || len(handles[0].IDs) != 1 {
		t.Fatalf("unexpected handles: %+v", handles)
	}
	// Inactive ID allocated and cached
	inact := handles[0].IDs[0].GetInActiveID()
	if inact.IsNil() {
		t.Fatalf("inactive ID not allocated")
	}
	// Blob for inactive exists
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, inact); len(ba) == 0 {
		t.Fatalf("inactive blob not added")
	}
	// Redis cached inactive
	var out btree.Node[PersonKey, Person]
	if ok2, _ := redis.GetStruct(ctx, nr.formatKey(inact.String()), &out); !ok2 {
		t.Fatalf("inactive cache not set")
	}
	// Registry updated with inactive ID
	if got := reg.(*mocks.Mock_vid_registry).Lookup[lid]; got.GetInActiveID() != inact {
		t.Fatalf("registry not updated with inactive: %+v", got)
	}
}

func Test_NodeRepository_CommitRemovedNodes_Succeeds(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	tx := &Transaction{registry: reg}
	nr := &nodeRepositoryBackend{transaction: tx}

	so := sop.StoreOptions{Name: "st_rem", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 5}
	// Seed registry with matching handle and version
	h := sop.NewHandle(lid)
	h.Version = 5
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}

	ok, handles, err := nr.commitRemovedNodes(ctx, nodes)
	if err != nil || !ok {
		t.Fatalf("commitRemovedNodes failed: ok=%v err=%v", ok, err)
	}
	if len(handles) != 1 || len(handles[0].IDs) != 1 {
		t.Fatalf("unexpected handles: %+v", handles)
	}
	// Registry marked deleted with timestamp
	got := reg.(*mocks.Mock_vid_registry).Lookup[lid]
	if !got.IsDeleted || got.WorkInProgressTimestamp == 0 {
		t.Fatalf("registry not marked deleted: %+v", got)
	}
}

func Test_NodeRepository_CommitNewRootNodes_FailsWhenExisting(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}
	nr := &nodeRepositoryBackend{transaction: tx}

	so := sop.StoreOptions{Name: "st_root", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	root := &btree.Node[PersonKey, Person]{ID: lid, Version: 1}
	// Seed registry to simulate existing root entry
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = sop.NewHandle(lid)
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{root}}}

	ok, _, err := nr.commitNewRootNodes(ctx, nodes)
	if err != nil || ok {
		t.Fatalf("expected commitNewRootNodes to return ok=false without error when root exists; got ok=%v err=%v", ok, err)
	}
}

func Test_NodeRepository_CommitUpdatedNodes_VersionMismatch_ReturnsFalse(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}
	nr := &nodeRepositoryBackend{transaction: tx}

	so := sop.StoreOptions{Name: "st_upd_conflict", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	// Node has version 3, but registry will have a different version to force mismatch
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 3}
	// Seed registry with mismatched version (e.g., 2)
	h := sop.NewHandle(lid)
	h.Version = 2
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}

	ok, handles, err := nr.commitUpdatedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("commitUpdatedNodes returned unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false due to version mismatch, got ok=true")
	}
	if handles != nil {
		t.Fatalf("expected no handles on conflict, got: %+v", handles)
	}
	// Ensure no inactive blob was written
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, lid); len(ba) != 0 {
		t.Fatalf("unexpected blob write on conflict")
	}
}

func Test_NodeRepository_CommitRemovedNodes_VersionMismatch_ReturnsFalse(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	tx := &Transaction{registry: reg}
	nr := &nodeRepositoryBackend{transaction: tx}

	so := sop.StoreOptions{Name: "st_rem_conflict", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	// Node has version 5, but registry shows version 4 to force mismatch
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 5}
	h := sop.NewHandle(lid)
	h.Version = 4
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}

	ok, handles, err := nr.commitRemovedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("commitRemovedNodes returned unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false due to version mismatch, got ok=true")
	}
	if handles != nil {
		t.Fatalf("expected nil handles on conflict, got: %+v", handles)
	}
	// Verify registry entry remains unchanged (not marked deleted)
	got := reg.(*mocks.Mock_vid_registry).Lookup[lid]
	if got.IsDeleted || got.WorkInProgressTimestamp != 0 {
		t.Fatalf("registry should be unchanged on conflict: %+v", got)
	}
}

func Test_NodeRepository_CommitUpdatedNodes_OngoingUpdate_ReturnsFalse(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}
	nr := &nodeRepositoryBackend{transaction: tx}

	so := sop.StoreOptions{Name: "st_upd_busy", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	// Node version matches registry handle version so we progress to AllocateID
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 7}
	// Seed a handle with both physical IDs in use to force AllocateID to return NilUUID
	h := sop.NewHandle(lid)
	h.Version = 7
	h.PhysicalIDB = sop.NewUUID()
	h.IsActiveIDB = true
	// Put A in use as well
	h.PhysicalIDA = sop.NewUUID()
	// Ensure not expired, so AllocateID won't be retried after ClearInactiveID branch
	h.WorkInProgressTimestamp = sop.Now().UnixMilli()
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h

	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}
	ok, handles, err := nr.commitUpdatedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected ok=false due to ongoing update (AllocateID fail), got ok=true")
	}
	if handles != nil {
		t.Fatalf("expected nil handles on ongoing update, got: %+v", handles)
	}
	// Ensure redis and blob store were not written for an inactive ID
	// Since AllocateID failed, there should be no new ID; check that no blob for any ID other than lid exists
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, lid); len(ba) != 0 {
		t.Fatalf("unexpected blob write for logical id on conflict")
	}
}
