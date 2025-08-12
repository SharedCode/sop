package fs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestRegistryReplicate_CloseError forces an rm.close() override error to assert lastErr surfaces only when no prior errors.
func TestRegistryReplicate_CloseError(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	baseA := t.TempDir()
	baseB := t.TempDir()
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	r := NewRegistry(true, MinimumModValue, rt, cache)
	defer r.Close()
	// Inject close error override.
	r.rmCloseOverride = func() error { return errors.New("close boom") }

	h := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "ce", CacheDuration: time.Minute, IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("seed add: %v", err)
	}
	// Successful operations, only close override should set lastErr.
	err = r.Replicate(ctx,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "ce", IDs: []sop.Handle{h}}},
		nil, nil, nil,
	)
	if err == nil || err.Error() != "close boom" {
		t.Fatalf("expected close boom error, got %v", err)
	}
}

// TestRegistryReplicate_CloseErrorIgnoredWhenPriorError ensures earlier add error masks close error (lastErr already set).
func TestRegistryReplicate_CloseErrorIgnoredWhenPriorError(t *testing.T) {
	ctx := context.Background()
	cache := mocks.NewMockClient()
	baseA := t.TempDir()
	baseB := t.TempDir()
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	rt, err := NewReplicationTracker(ctx, []string{baseA, baseB}, true, cache)
	if err != nil {
		t.Fatalf("tracker: %v", err)
	}

	r := NewRegistry(true, MinimumModValue, rt, cache)
	defer r.Close()
	// Induce prior error by referencing a table that will cause replication add to fail: make passive readonly after seeding.
	// Instead we force replication failure by clearing replication flag mid-flight via tracker override after first add call.
	called := false
	r.rmCloseOverride = func() error { return errors.New("close boom") }
	// Seed one handle so replicate has work.
	h := sop.NewHandle(sop.NewUUID())
	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{{RegistryTable: "ce2", CacheDuration: time.Minute, IDs: []sop.Handle{h}}}); err != nil {
		t.Fatalf("seed add: %v", err)
	}
	// Monkey patch replicationTracker handleFailedToReplicate to mark error before close by triggering failure via nonexistent table hash mismatch.
	// Simplify: call Replicate with removed handles referencing unknown ID to force remove error earlier.
	badID := sop.NewUUID()
	err = r.Replicate(ctx,
		nil,
		nil,
		nil,
		[]sop.RegistryPayload[sop.Handle]{{RegistryTable: "ce2", IDs: []sop.Handle{{LogicalID: badID}}}},
	)
	// We cannot guarantee specific error text; assert it is not the close override because prior error should mask.
	if err == nil {
		t.Fatalf("expected prior replication error")
	}
	if err.Error() == "close boom" {
		t.Fatalf("expected prior error to mask close error, got close override")
	}
	_ = called
}
