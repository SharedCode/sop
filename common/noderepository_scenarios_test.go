package common

// Consolidated from: noderepository_helpers_test.go, noderepository_commit_test.go,
// noderepository_frontend_test.go, noderepository_rollback_test.go, noderepository_more_rollback_test.go
// Scenario grouping keeps prior test function names intact.

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

// ---- Helpers scenarios ----
func Test_ConvertHelpers_MapNodesToPayloads(t *testing.T) { // from noderepository_helpers_test.go
	ctx := context.Background()
	_ = ctx
	so := sop.StoreOptions{Name: "nr_helpers", SlotLength: 8, IsValueDataInNodeSegment: true}
	si := sop.NewStoreInfo(so)
	n1 := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
	n2 := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 2}
	nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{{First: si, Second: []interface{}{n1, n2}}}
	bibs := convertToBlobRequestPayload(nodes)
	if len(bibs) != 1 || len(bibs[0].Blobs) != 2 {
		t.Fatalf("unexpected blob payload shape: %+v", bibs)
	}
	if bibs[0].Blobs[0] != n1.ID || bibs[0].Blobs[1] != n2.ID {
		t.Fatalf("blob IDs mismatch: %+v", bibs[0].Blobs)
	}
	vids := convertToRegistryRequestPayload(nodes)
	if len(vids) != 1 || len(vids[0].IDs) != 2 {
		t.Fatalf("unexpected registry payload shape: %+v", vids)
	}
	if vids[0].IDs[0] != n1.ID || vids[0].IDs[1] != n2.ID {
		t.Fatalf("registry IDs mismatch: %+v", vids[0].IDs)
	}
}

func Test_ExtractInactiveBlobsIDs(t *testing.T) { // from noderepository_helpers_test.go
	l1 := sop.NewUUID()
	l2 := sop.NewUUID()
	h1 := sop.NewHandle(l1)
	h1.PhysicalIDB = sop.NewUUID()
	h1.IsActiveIDB = true
	h2 := sop.NewHandle(l2)
	payload := []sop.RegistryPayload[sop.Handle]{{BlobTable: "tbl", IDs: []sop.Handle{h1, h2}}}
	b := extractInactiveBlobsIDs(payload)
	if len(b) != 1 || len(b[0].Blobs) != 1 || b[0].Blobs[0] != h1.PhysicalIDA {
		t.Fatalf("unexpected inactive blobs: %+v", b)
	}
}

func Test_NodeRepository_ActivateAndTouch(t *testing.T) { // from noderepository_helpers_test.go
	h1 := sop.NewHandle(sop.NewUUID())
	h1.PhysicalIDB = sop.NewUUID()
	h1.IsActiveIDB = false
	h1.Version = 5
	h2 := sop.NewHandle(sop.NewUUID())
	h2.PhysicalIDB = sop.NewUUID()
	h2.IsActiveIDB = true
	h2.Version = 2
	h2.WorkInProgressTimestamp = 7
	set := []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{h1, h2}}}
	nr := &nodeRepositoryBackend{}
	out, err := nr.activateInactiveNodes(set)
	if err != nil {
		t.Fatalf("activateInactiveNodes error: %v", err)
	}
	if !out[0].IDs[0].IsActiveIDB || out[0].IDs[0].Version != 6 || out[0].IDs[0].WorkInProgressTimestamp != 1 {
		t.Fatalf("unexpected h1 after activate: %+v", out[0].IDs[0])
	}
	if out[0].IDs[1].IsActiveIDB || out[0].IDs[1].Version != 3 || out[0].IDs[1].WorkInProgressTimestamp != 1 {
		t.Fatalf("unexpected h2 after activate: %+v", out[0].IDs[1])
	}
	out2, err := nr.touchNodes(out)
	if err != nil {
		t.Fatalf("touchNodes error: %v", err)
	}
	if out2[0].IDs[0].Version != 7 || out2[0].IDs[0].WorkInProgressTimestamp != 0 {
		t.Fatalf("unexpected h1 after touch: %+v", out2[0].IDs[0])
	}
	if out2[0].IDs[1].Version != 4 || out2[0].IDs[1].WorkInProgressTimestamp != 0 {
		t.Fatalf("unexpected h2 after touch: %+v", out2[0].IDs[1])
	}
}

// ---- Commit scenarios ----
func Test_NodeRepository_CommitAddedNodes(t *testing.T) { // from noderepository_commit_test.go
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
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, n.ID); len(ba) == 0 {
		t.Fatalf("blob not persisted for added node")
	}
	var out btree.Node[PersonKey, Person]
	if ok, _ := redis.GetStruct(ctx, nr.formatKey(n.ID.String()), &out); !ok {
		t.Fatalf("redis cache not set")
	}
	got, _ := reg.Get(ctx, []sop.RegistryPayload[sop.UUID]{{RegistryTable: si.RegistryTable, IDs: []sop.UUID{n.ID}}})
	if len(got) != 1 || len(got[0].IDs) != 1 || got[0].IDs[0].Version != 1 {
		t.Fatalf("registry handle wrong: %+v", got)
	}
}

func Test_NodeRepository_CommitUpdatedNodes_Succeeds(t *testing.T) { // from noderepository_commit_test.go
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
	inact := handles[0].IDs[0].GetInActiveID()
	if inact.IsNil() {
		t.Fatalf("inactive ID not allocated")
	}
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, inact); len(ba) == 0 {
		t.Fatalf("inactive blob not added")
	}
	var out btree.Node[PersonKey, Person]
	if ok2, _ := redis.GetStruct(ctx, nr.formatKey(inact.String()), &out); !ok2 {
		t.Fatalf("inactive cache not set")
	}
	if reg.(*mocks.Mock_vid_registry).Lookup[lid].GetInActiveID() != inact {
		t.Fatalf("registry not updated with inactive")
	}
}

func Test_NodeRepository_CommitRemovedNodes_Succeeds(t *testing.T) { // from noderepository_commit_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	tx := &Transaction{registry: reg}
	nr := &nodeRepositoryBackend{transaction: tx}
	so := sop.StoreOptions{Name: "st_rem", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 5}
	h := sop.NewHandle(lid)
	h.Version = 5
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}
	ok, handles, err := nr.commitRemovedNodes(ctx, nodes)
	if err != nil || !ok {
		t.Fatalf("commitRemovedNodes failed: %v", err)
	}
	if len(handles) != 1 || len(handles[0].IDs) != 1 {
		t.Fatalf("unexpected handles: %+v", handles)
	}
	got := reg.(*mocks.Mock_vid_registry).Lookup[lid]
	if !got.IsDeleted || got.WorkInProgressTimestamp == 0 {
		t.Fatalf("registry not marked deleted: %+v", got)
	}
}

func Test_NodeRepository_CommitNewRootNodes_FailsWhenExisting(t *testing.T) { // from noderepository_commit_test.go
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
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = sop.NewHandle(lid)
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{root}}}
	ok, _, err := nr.commitNewRootNodes(ctx, nodes)
	if err != nil || ok {
		t.Fatalf("expected ok=false without error when root exists; got ok=%v err=%v", ok, err)
	}
}

func Test_NodeRepository_CommitUpdatedNodes_VersionMismatch_ReturnsFalse(t *testing.T) { // from noderepository_commit_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}
	nr := &nodeRepositoryBackend{transaction: tx}
	so := sop.StoreOptions{Name: "st_upd_conflict", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 3}
	h := sop.NewHandle(lid)
	h.Version = 2
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}
	ok, handles, err := nr.commitUpdatedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("commitUpdatedNodes unexpected error: %v", err)
	}
	if ok || handles != nil {
		t.Fatalf("expected conflict (ok=false, handles nil)")
	}
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, lid); len(ba) != 0 {
		t.Fatalf("unexpected blob write on conflict")
	}
}

func Test_NodeRepository_CommitRemovedNodes_VersionMismatch_ReturnsFalse(t *testing.T) { // from noderepository_commit_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	tx := &Transaction{registry: reg}
	nr := &nodeRepositoryBackend{transaction: tx}
	so := sop.StoreOptions{Name: "st_rem_conflict", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 5}
	h := sop.NewHandle(lid)
	h.Version = 4
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}
	ok, handles, err := nr.commitRemovedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("commitRemovedNodes unexpected error: %v", err)
	}
	if ok || handles != nil {
		t.Fatalf("expected version mismatch conflict")
	}
	got := reg.(*mocks.Mock_vid_registry).Lookup[lid]
	if got.IsDeleted || got.WorkInProgressTimestamp != 0 {
		t.Fatalf("registry should be unchanged on conflict: %+v", got)
	}
}

func Test_NodeRepository_CommitUpdatedNodes_OngoingUpdate_ReturnsFalse(t *testing.T) { // from noderepository_commit_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}
	nr := &nodeRepositoryBackend{transaction: tx}
	so := sop.StoreOptions{Name: "st_upd_busy", SlotLength: 2}
	si := sop.NewStoreInfo(so)
	lid := sop.NewUUID()
	n := &btree.Node[PersonKey, Person]{ID: lid, Version: 7}
	h := sop.NewHandle(lid)
	h.Version = 7
	h.PhysicalIDB = sop.NewUUID()
	h.IsActiveIDB = true
	h.PhysicalIDA = sop.NewUUID()
	h.WorkInProgressTimestamp = sop.Now().UnixMilli()
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nodes := []sop.Tuple[*sop.StoreInfo, []any]{{First: si, Second: []any{n}}}
	ok, handles, err := nr.commitUpdatedNodes(ctx, nodes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok || handles != nil {
		t.Fatalf("expected ongoing update conflict")
	}
	if ba, _ := blobs.GetOne(ctx, si.BlobTable, lid); len(ba) != 0 {
		t.Fatalf("unexpected blob write for logical id")
	}
}

// ---- Frontend wrapper scenarios ----
func Test_NodeRepository_Frontend_Wrappers(t *testing.T) { // from noderepository_frontend_test.go
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	cache.NewGlobalCache(l2, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	reg := mocks.NewMockRegistry(false)
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{registry: reg, l2Cache: l2, blobStore: blobs, l1Cache: cache.GetGlobalCache()}
	so := sop.StoreOptions{Name: "nr_front", SlotLength: 4, IsValueDataInNodeSegment: true}
	si := sop.NewStoreInfo(so)
	nr := newNodeRepository[PersonKey, Person](tx, si)
	n := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
	nr.Add(n)
	if v, ok := nr.localCache[n.ID]; !ok || v.action != addAction {
		t.Fatalf("Add did not mark addAction")
	}
	n.Version = 2
	nr.Update(n)
	if v := nr.localCache[n.ID]; v.action != addAction {
		t.Fatalf("Update should retain addAction")
	}
	nr.readNodesCache.Set([]sop.KeyValuePair[sop.UUID, any]{{Key: n.ID, Value: n}})
	nr.Fetched(n.ID)
	if v, ok := nr.localCache[n.ID]; !ok || v.action != getAction || v.node == nil {
		t.Fatalf("Fetched did not migrate node")
	}
	nr.Remove(n.ID)
	if v := nr.localCache[n.ID]; v.action != removeAction {
		t.Fatalf("Remove did not set removeAction")
	}
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.Version = 3
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	nb := &btree.Node[PersonKey, Person]{ID: h.GetActiveID(), Version: h.Version}
	if err := blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: si.BlobTable, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: nb.ID, Value: mustNode(nb)}}}}); err != nil {
		t.Fatalf("seed blob: %v", err)
	}
	got, err := nr.Get(ctx, lid)
	if err != nil || got == nil || got.ID != nb.ID || got.Version != h.Version {
		t.Fatalf("Get unexpected result; err=%v got=%+v", err, got)
	}
	var out btree.Node[PersonKey, Person]
	if nr.l1Cache.GetNodeFromMRU(h, &out) == nil || out.ID != nb.ID || out.Version != h.Version {
		t.Fatalf("expected node cached in L1 MRU")
	}
	_, _ = l2.Delete(ctx, []string{cache.FormatNodeKey(nb.ID.String())})
	_, _ = l2.Delete(ctx, []string{l2.FormatLockKey(lid.String())})
	_ = l2.Set(ctx, l2.FormatLockKey("cleanup"), sop.NewUUID().String(), time.Second)
}

// ---- Rollback scenarios ----
func Test_NodeRepository_RollbackAddedNodes(t *testing.T) { // from noderepository_rollback_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}}
	id := sop.NewUUID()
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rtbl", IDs: []sop.UUID{id}}}
	bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: "btbl", Blobs: []sop.UUID{id}}}
	_ = redis.SetStruct(ctx, nr.formatKey(id.String()), &btree.Node[PersonKey, Person]{ID: id}, time.Minute)
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "btbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("x")}}}})
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{sop.NewHandle(id)}}})
	if err := nr.rollbackAddedNodes(ctx, sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}); err != nil {
		t.Fatalf("rollbackAddedNodes error: %v", err)
	}
	if ba, _ := blobs.GetOne(ctx, "btbl", id); len(ba) != 0 {
		t.Fatalf("blob not removed: %s", id.String())
	}
	var out btree.Node[PersonKey, Person]
	if ok, _ := redis.GetStruct(ctx, nr.formatKey(id.String()), &out); ok {
		t.Fatalf("cache not removed: %s", id.String())
	}
}

func Test_NodeRepository_RollbackUpdatedNodes(t *testing.T) { // from noderepository_rollback_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}}
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.PhysicalIDB = sop.NewUUID()
	h.IsActiveIDB = true
	h.WorkInProgressTimestamp = 123
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "btbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: h.PhysicalIDA, Value: []byte("v")}}}})
	_ = redis.SetStruct(ctx, nr.formatKey(h.PhysicalIDA.String()), &btree.Node[PersonKey, Person]{ID: h.PhysicalIDA}, time.Minute)
	vids := []sop.RegistryPayload[sop.UUID]{{BlobTable: "btbl", RegistryTable: "rtbl", IDs: []sop.UUID{lid}}}
	if err := nr.rollbackUpdatedNodes(ctx, true, vids); err != nil {
		t.Fatalf("rollbackUpdatedNodes error: %v", err)
	}
	if ba, _ := blobs.GetOne(ctx, "btbl", h.PhysicalIDA); len(ba) != 0 {
		t.Fatalf("inactive blob not removed")
	}
	var out btree.Node[PersonKey, Person]
	if ok, _ := redis.GetStruct(ctx, nr.formatKey(h.PhysicalIDA.String()), &out); ok {
		t.Fatalf("inactive cache not removed")
	}
}

func Test_NodeRepository_RollbackRemovedNodes(t *testing.T) { // from noderepository_rollback_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg}}
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.IsDeleted = true
	h.WorkInProgressTimestamp = 999
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rtbl", IDs: []sop.UUID{lid}}}
	if err := nr.rollbackRemovedNodes(ctx, true, vids); err != nil {
		t.Fatalf("rollbackRemovedNodes error: %v", err)
	}
	got := reg.(*mocks.Mock_vid_registry).Lookup[lid]
	if got.IsDeleted || got.WorkInProgressTimestamp != 0 {
		t.Fatalf("flags not cleared: %+v", got)
	}
}

// Additional branch coverage consolidated from locks_and_registry_test.go
func Test_NodeRepository_RollbackRemovedNodes_Unlocked_Branch(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg}}
	// Seed two handles with flags set
	lid1 := sop.NewUUID()
	lid2 := sop.NewUUID()
	h1 := sop.NewHandle(lid1)
	h1.IsDeleted = true
	h2 := sop.NewHandle(lid2)
	h2.WorkInProgressTimestamp = 123
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{h1, h2}}})

	// nodesAreLocked=false uses Update (not UpdateNoLocks)
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt", IDs: []sop.UUID{lid1, lid2}}}
	if err := nr.rollbackRemovedNodes(ctx, false, vids); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	got, _ := reg.Get(ctx, vids)
	if len(got) > 0 {
		for _, gh := range got[0].IDs {
			if gh.LogicalID.Compare(lid1) == 0 || gh.LogicalID.Compare(lid2) == 0 {
				if gh.IsDeleted || gh.WorkInProgressTimestamp != 0 {
					t.Fatalf("flags not cleared: %+v", gh)
				}
			}
		}
	}
}

func Test_NodeRepository_RemoveNodes(t *testing.T) { // from noderepository_rollback_test.go
	ctx := context.Background()
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	nr := &nodeRepositoryBackend{transaction: &Transaction{l2Cache: redis, blobStore: blobs}}
	id := sop.NewUUID()
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "btbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("v")}}}})
	_ = redis.SetStruct(ctx, nr.formatKey(id.String()), &btree.Node[PersonKey, Person]{ID: id}, time.Minute)
	bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: "btbl", Blobs: []sop.UUID{id}}}
	if err := nr.removeNodes(ctx, bibs); err != nil {
		t.Fatalf("removeNodes error: %v", err)
	}
	if ba, _ := blobs.GetOne(ctx, "btbl", id); len(ba) != 0 {
		t.Fatalf("blob not removed")
	}
	var out btree.Node[PersonKey, Person]
	if ok, _ := redis.GetStruct(ctx, nr.formatKey(id.String()), &out); ok {
		t.Fatalf("cache not removed")
	}
}

func Test_NodeRepository_RollbackNewRootNodes_Various(t *testing.T) { // from noderepository_more_rollback_test.go
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tl.committedState = beforeFinalize
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg, l2Cache: redis, blobStore: blobs, logger: tl}}
	id1 := sop.NewUUID()
	id2 := sop.NewUUID()
	vids := []sop.RegistryPayload[sop.UUID]{{RegistryTable: "rt", IDs: []sop.UUID{id1}}, {RegistryTable: "rt", IDs: []sop.UUID{id2}}}
	bibs := []sop.BlobsPayload[sop.UUID]{{BlobTable: "bt", Blobs: []sop.UUID{id1}}, {BlobTable: "bt", Blobs: []sop.UUID{id2}}}
	_ = redis.SetStruct(ctx, nr.formatKey(id1.String()), &btree.Node[PersonKey, Person]{ID: id1}, time.Minute)
	_ = redis.SetStruct(ctx, nr.formatKey(id2.String()), &btree.Node[PersonKey, Person]{ID: id2}, time.Minute)
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "bt", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id1, Value: []byte("a")}, {Key: id2, Value: []byte("b")}}}})
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(id1), sop.NewHandle(id2)}}})
	if err := nr.rollbackNewRootNodes(ctx, sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}); err != nil {
		t.Fatalf("rollbackNewRootNodes err: %v", err)
	}
	for _, id := range []sop.UUID{id1, id2} {
		if ba, _ := blobs.GetOne(ctx, "bt", id); len(ba) != 0 {
			t.Fatalf("blob not removed: %s", id.String())
		}
		var out btree.Node[PersonKey, Person]
		if ok, _ := redis.GetStruct(ctx, nr.formatKey(id.String()), &out); ok {
			t.Fatalf("cache not removed: %s", id.String())
		}
	}
}

// Marshal helper preserved from original frontend file.
func mustNode(n *btree.Node[PersonKey, Person]) []byte {
	b, _ := encoding.BlobMarshaler.Marshal(n)
	return b
}
