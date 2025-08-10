package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

func Test_NodeRepository_RollbackAddedNodes(t *testing.T) {
	ctx := context.Background()
	// Prepare mocked transaction deps
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}}

	// Build rollback data: one node
	id := sop.NewUUID()
	vids := []sop.RegistryPayload[sop.UUID]{
		{RegistryTable: "rtbl", IDs: []sop.UUID{id}},
	}
	bibs := []sop.BlobsPayload[sop.UUID]{
		{BlobTable: "btbl", Blobs: []sop.UUID{id}},
	}
	// Seed cache and blob to be removed
	_ = redis.SetStruct(ctx, nr.formatKey(id.String()), &btree.Node[PersonKey, Person]{ID: id}, time.Minute)
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "btbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: []byte("x")}}}})
	// Also seed registry and ensure Remove works
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{sop.NewHandle(id)}}})

	if err := nr.rollbackAddedNodes(ctx, sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}); err != nil {
		t.Fatalf("rollbackAddedNodes error: %v", err)
	}
	// Verify blob removed
	if ba, _ := blobs.GetOne(ctx, "btbl", id); len(ba) != 0 {
		t.Fatalf("blob not removed for %s", id.String())
	}
	// Verify cache removed
	var out btree.Node[PersonKey, Person]
	if ok, _ := redis.GetStruct(ctx, nr.formatKey(id.String()), &out); ok {
		t.Fatalf("cache not removed for %s", id.String())
	}
}

func Test_NodeRepository_RollbackUpdatedNodes(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg, l2Cache: redis, blobStore: blobs}}

	// Logical ID with inactive set; registry.Get will return this handle
	lid := sop.NewUUID()
	h := sop.NewHandle(lid)
	h.PhysicalIDB = sop.NewUUID()
	h.IsActiveIDB = true // inactive is A
	h.WorkInProgressTimestamp = 123
	// Pre-populate registry
	reg.(*mocks.Mock_vid_registry).Lookup[lid] = h
	// Pre-populate blob for inactive ID and cache
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "btbl", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: h.PhysicalIDA, Value: []byte("v")}}}})
	_ = redis.SetStruct(ctx, nr.formatKey(h.PhysicalIDA.String()), &btree.Node[PersonKey, Person]{ID: h.PhysicalIDA}, time.Minute)

	vids := []sop.RegistryPayload[sop.UUID]{{BlobTable: "btbl", RegistryTable: "rtbl", IDs: []sop.UUID{lid}}}
	if err := nr.rollbackUpdatedNodes(ctx, true, vids); err != nil {
		t.Fatalf("rollbackUpdatedNodes error: %v", err)
	}
	// Blob removed
	if ba, _ := blobs.GetOne(ctx, "btbl", h.PhysicalIDA); len(ba) != 0 {
		t.Fatalf("inactive blob not removed")
	}
	// Cache removed
	var out btree.Node[PersonKey, Person]
	if ok, _ := redis.GetStruct(ctx, nr.formatKey(h.PhysicalIDA.String()), &out); ok {
		t.Fatalf("inactive cache not removed")
	}
}

func Test_NodeRepository_RollbackRemovedNodes(t *testing.T) {
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
	// Validate flags cleared
	got := reg.(*mocks.Mock_vid_registry).Lookup[lid]
	if got.IsDeleted || got.WorkInProgressTimestamp != 0 {
		t.Fatalf("flags not cleared: %+v", got)
	}
}

func Test_NodeRepository_RemoveNodes(t *testing.T) {
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
