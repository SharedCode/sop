package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

// Exercises rollbackNewRootNodes when logger state indicates registration happened and resources exist.
func Test_NodeRepository_RollbackNewRootNodes_Various(t *testing.T) {
	ctx := context.Background()
	reg := mocks.NewMockRegistry(false)
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	// Simulate we already logged past commitNewRootNodes
	tl.committedState = beforeFinalize
	nr := &nodeRepositoryBackend{transaction: &Transaction{registry: reg, l2Cache: redis, blobStore: blobs, logger: tl}}

	// Build rollback payload for two nodes
	id1 := sop.NewUUID()
	id2 := sop.NewUUID()
	vids := []sop.RegistryPayload[sop.UUID]{
		{RegistryTable: "rt", IDs: []sop.UUID{id1}},
		{RegistryTable: "rt", IDs: []sop.UUID{id2}},
	}
	bibs := []sop.BlobsPayload[sop.UUID]{
		{BlobTable: "bt", Blobs: []sop.UUID{id1}},
		{BlobTable: "bt", Blobs: []sop.UUID{id2}},
	}
	// Seed cache and blobs
	_ = redis.SetStruct(ctx, nr.formatKey(id1.String()), &btree.Node[PersonKey, Person]{ID: id1}, time.Minute)
	_ = redis.SetStruct(ctx, nr.formatKey(id2.String()), &btree.Node[PersonKey, Person]{ID: id2}, time.Minute)
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: "bt", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id1, Value: []byte("a")}, {Key: id2, Value: []byte("b")}}}})
	// Seed registry to allow Remove to have effect when committedState threshold passed
	_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rt", IDs: []sop.Handle{sop.NewHandle(id1), sop.NewHandle(id2)}}})

	if err := nr.rollbackNewRootNodes(ctx, sop.Tuple[[]sop.RegistryPayload[sop.UUID], []sop.BlobsPayload[sop.UUID]]{First: vids, Second: bibs}); err != nil {
		t.Fatalf("rollbackNewRootNodes err: %v", err)
	}
	// Assert blobs gone and cache keys deleted
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
