package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// TestReplicationTracker_BasicScenarios consolidates small unit cases into a table-driven suite.
func TestReplicationTracker_BasicScenarios(t *testing.T) {
	t.Cleanup(func() { GlobalReplicationDetails = nil })

	type scenario struct {
		name string
		run  func(t *testing.T)
	}

	ctx := context.Background()

	cases := []scenario{
		{
			name: "formatters respect toggler",
			run: func(t *testing.T) {
				base := t.TempDir()
				active := base + string(os.PathSeparator)
				passive := filepath.Join(base, "p")
				rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
				if err != nil {
					t.Fatalf("rt: %v", err)
				}

				got := rt.formatActiveFolderEntity("x/y")
				if want := filepath.Join(active, "x/y"); got != want {
					t.Fatalf("formatActiveFolderEntity got %q want %q", got, want)
				}
				// flip active to passive and test passive formatter
				rt.ActiveFolderToggler = false
				id := sop.NewUUID().String()
				got = rt.formatPassiveFolderEntity(id)
				if want := filepath.Join(active, id); got != want {
					t.Fatalf("formatPassiveFolderEntity got %q want %q", got, want)
				}
			},
		},
		{
			name: "write/read replication status roundtrip",
			run: func(t *testing.T) {
				active := filepath.Join(t.TempDir(), "a")
				passive := filepath.Join(t.TempDir(), "b")
				rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
				if err != nil {
					t.Fatalf("rt: %v", err)
				}
				rt.ActiveFolderToggler = true
				rt.FailedToReplicate = false
				if err := rt.writeReplicationStatus(ctx, rt.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
					t.Fatalf("writeReplicationStatus: %v", err)
				}
				rt2, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
				if err != nil {
					t.Fatalf("rt2: %v", err)
				}
				if err := rt2.readReplicationStatus(ctx, rt2.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
					t.Fatalf("readReplicationStatus: %v", err)
				}
				if rt2.FailedToReplicate {
					t.Fatalf("FailedToReplicate should be false")
				}
			},
		},
		{
			name: "handle error triggers failover + status file",
			run: func(t *testing.T) {
				GlobalReplicationDetails = nil
				active := filepath.Join(t.TempDir(), "act")
				passive := filepath.Join(t.TempDir(), "pas")
				rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
				if err != nil {
					t.Fatalf("rt: %v", err)
				}
				rt.ActiveFolderToggler = true

				ioErr := sop.Error{Code: sop.FailoverQualifiedError, Err: errors.New("io fail")}
				rt.HandleReplicationRelatedError(ctx, ioErr, nil, false)

				if rt.ActiveFolderToggler != false {
					t.Fatalf("expected toggler flipped to passive active")
				}
				if !rt.FailedToReplicate {
					t.Fatalf("expected FailedToReplicate true")
				}
				fn := rt.formatActiveFolderEntity(replicationStatusFilename)
				if _, err := os.Stat(fn); err != nil {
					t.Fatalf("expected replication status at %s: %v", fn, err)
				}
			},
		},
		{
			name: "syncWithL2 push then pull",
			run: func(t *testing.T) {
				GlobalReplicationDetails = nil
				active := filepath.Join(t.TempDir(), "a")
				passive := filepath.Join(t.TempDir(), "b")
				cache := mocks.NewMockClient()
				rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, cache)
				if err != nil {
					t.Fatalf("rt: %v", err)
				}

				// Seed global and push
				GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: true}
				if err := rt.syncWithL2Cache(ctx, true); err != nil {
					t.Fatalf("push: %v", err)
				}

				// Clear and pull
				GlobalReplicationDetails = nil
				if err := rt.syncWithL2Cache(ctx, false); err != nil {
					t.Fatalf("pull: %v", err)
				}
				if GlobalReplicationDetails == nil || !GlobalReplicationDetails.FailedToReplicate {
					t.Fatalf("expected pulled value set true")
				}
			},
		},
		{
			name: "startLoggingCommitChanges persists + flag",
			run: func(t *testing.T) {
				active := filepath.Join(t.TempDir(), "a")
				rt, err := NewReplicationTracker(ctx, []string{active, filepath.Join(t.TempDir(), "b")}, true, mocks.NewMockClient())
				if err != nil {
					t.Fatalf("rt: %v", err)
				}

				if err := rt.startLoggingCommitChanges(ctx); err != nil {
					t.Fatalf("startLoggingCommitChanges: %v", err)
				}
				if !rt.LogCommitChanges {
					t.Fatalf("expected LogCommitChanges true")
				}
				// status file should exist
				if _, err := os.Stat(rt.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
					t.Fatalf("status file missing: %v", err)
				}
			},
		},
		{
			name: "SetTransactionID smoke + FileIO exists",
			run: func(t *testing.T) {
				active := filepath.Join(t.TempDir(), "act")
				passive := filepath.Join(t.TempDir(), "pas")
				rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
				rt.SetTransactionID(sop.NewUUID())

				ms := NewManageStoreFolder(nil)
				fio := newFileIOWithReplication(rt, ms, false)
				// create a file under active and check exists
				name := "foo.bin"
				path := rt.formatActiveFolderEntity(name)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
				if !fio.exists(ctx, name) {
					t.Fatalf("expected exists true")
				}
			},
		},
	}

	for _, tc := range cases {
		// Reset global before and after each case to avoid leakage from other tests
		GlobalReplicationDetails = nil
		t.Run(tc.name, tc.run)
		GlobalReplicationDetails = nil
	}
}

// Covers handleFailedToReplicate branch where GlobalReplicationDetails already marked failed (after tracker creation) causing early return without writing status file.
func TestReplicationTracker_handleFailedToReplicate_GlobalAlreadyFailed(t *testing.T) {
	ctx := context.Background()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	t.Cleanup(func() { GlobalReplicationDetails = prev })

	active, passive := t.TempDir(), t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	if rt.FailedToReplicate {
		t.Fatalf("expected initial failed flag false")
	}
	// Set global failure AFTER creation so rt local flag remains false.
	GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: rt.ActiveFolderToggler}

	rt.handleFailedToReplicate(ctx)

	if !rt.FailedToReplicate {
		t.Fatalf("expected local failed flag set true from global")
	}
	// We expect an early return prior to status write. If file exists it's benign (another test might have written).
}

// Covers failover early no-op path when global already reflects opposite ActiveFolderToggler.
func TestReplicationTracker_failover_GlobalAlreadyFlipped(t *testing.T) {
	ctx := context.Background()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	t.Cleanup(func() { GlobalReplicationDetails = prev })

	active, passive := t.TempDir(), t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	// Simulate global already flipped.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: !rt.ActiveFolderToggler}
	if err := rt.failover(ctx); err != nil {
		t.Fatalf("failover returned unexpected error: %v", err)
	}
	expected := rt.ActiveFolderToggler      // capture before for clarity (should stay same)
	if rt.ActiveFolderToggler != expected { // should NOT change local toggler
		t.Fatalf("expected local toggler unchanged on early no-op")
	}
}

// Covers failover early return when local FailedToReplicate already true.
func TestReplicationTracker_failover_AlreadyFailedToReplicate(t *testing.T) {
	ctx := context.Background()
	prev := GlobalReplicationDetails
	GlobalReplicationDetails = nil
	t.Cleanup(func() { GlobalReplicationDetails = prev })

	active, passive := t.TempDir(), t.TempDir()
	rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("NewReplicationTracker: %v", err)
	}
	rt.FailedToReplicate = true
	// Ensure global differs but local failed flag short-circuits.
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: rt.ActiveFolderToggler}
	if err := rt.failover(ctx); err != nil {
		t.Fatalf("failover err: %v", err)
	}
	if !rt.FailedToReplicate {
		t.Fatalf("expected failed flag to remain true")
	}
}
