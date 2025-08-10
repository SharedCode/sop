package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cache"
)

func Test_Transaction_Begin_Errors(t *testing.T) {
	tr := &Transaction{phaseDone: -1}
	if err := tr.Begin(); err != nil {
		t.Fatalf("first begin should succeed: %v", err)
	}
	if err := tr.Begin(); err == nil {
		t.Fatalf("second begin should fail")
	}

	tr2 := &Transaction{phaseDone: 2}
	if err := tr2.Begin(); err == nil {
		t.Fatalf("begin after done should fail")
	}
}

func Test_Transaction_Phase2Commit_Preconditions(t *testing.T) {
	ctx := context.Background()
	tr := &Transaction{phaseDone: -1}
	if err := tr.Phase2Commit(ctx); err == nil {
		t.Fatalf("Phase2Commit should fail when not begun")
	}

	tr2 := &Transaction{phaseDone: -1}
	if err := tr2.Begin(); err != nil {
		t.Fatalf("begin err: %v", err)
	}
	// phaseDone==0 after Begin
	if err := tr2.Phase2Commit(ctx); err == nil {
		t.Fatalf("Phase2Commit should error when phase 1 not invoked")
	}
}

func Test_Transaction_Rollback_Preconditions(t *testing.T) {
	ctx := context.Background()
	tr := &Transaction{phaseDone: -1}
	if err := tr.Rollback(ctx, nil); err == nil {
		t.Fatalf("Rollback should fail when not begun")
	}
}

func Test_DeleteObsoleteEntries_Smoke(t *testing.T) {
	ctx := context.Background()
	tr := &Transaction{blobStore: mockNodeBlobStore, registry: mockRegistry, l1Cache: cache.GetGlobalCache()}
	// One deleted registry ID and one unused node blob
	del := []sop.RegistryPayload[sop.UUID]{{IDs: []sop.UUID{sop.NewUUID()}}}
	unused := []sop.BlobsPayload[sop.UUID]{{Blobs: []sop.UUID{sop.NewUUID()}}}
	if err := tr.deleteObsoleteEntries(ctx, del, unused); err != nil {
		t.Fatalf("deleteObsoleteEntries err: %v", err)
	}
}
