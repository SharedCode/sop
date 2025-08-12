package fs

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistryReplicate_SuccessPath exercises Replicate with no induced errors so the success + rm.close() branch is covered.
func TestRegistryReplicate_SuccessPath(t *testing.T) {
	ctx := context.Background()
	l2 := mocks.NewMockClient()
	baseA := t.TempDir()
	baseB := t.TempDir()
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, true, l2)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	reg := NewRegistry(true, MinimumModValue, rt, l2)
	defer reg.Close()

	// Seed initial handles to ensure add/set/remove sequences have something to work with.
	hRoot := sop.NewHandle(sop.NewUUID())
	hAdd := sop.NewHandle(sop.NewUUID())
	hUpd := sop.NewHandle(sop.NewUUID())
	hDel := sop.NewHandle(sop.NewUUID())
	if err := reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgsucc", CacheDuration: time.Minute, IDs: []sop.Handle{hRoot, hAdd, hUpd, hDel}}}); err != nil {
		t.Fatalf("seed add: %v", err)
	}

	// Prepare update (same logical ID, different struct copy) and delete targets.
	hUpd2 := hUpd // reference same ID to update via set
	// Invoke replicate: one new root, one added, one updated, one removed.
	if err := reg.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgsucc", IDs: []sop.Handle{hRoot}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgsucc", IDs: []sop.Handle{hAdd}}},
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "rgsucc", IDs: []sop.Handle{hUpd2}}},
		nil, // no removals to keep passive apply simple
	); err != nil {
		t.Fatalf("replicate success path unexpected error: %v", err)
	}
}

// TestConvertToKvp covers the simple mapping utility.
func TestConvertToKvp(t *testing.T) {
	h1 := sop.NewHandle(sop.NewUUID())
	h2 := sop.NewHandle(sop.NewUUID())
	kv := convertToKvp([]sop.Handle{h1, h2})
	if len(kv) != 2 || kv[0].Key != h1.LogicalID || kv[1].Key != h2.LogicalID {
		t.Fatalf("convertToKvp produced unexpected mapping: %+v", kv)
	}
}
