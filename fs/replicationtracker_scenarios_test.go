package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

// TestReplicationTracker_Scenarios consolidates replication tracker tests. Each subtest resets globals
// to avoid state leakage that previously required many small *_cases files.
func TestReplicationTracker_Scenarios(t *testing.T) {
	type scenario struct {
		name string
		run  func(t *testing.T)
	}
	scenarios := []scenario{
		{name: "HandleReplicationRelatedErrorFailover", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			b1 := t.TempDir()
			b2 := t.TempDir()
			GlobalReplicationDetails = nil
			rt, err := NewReplicationTracker(ctx, []string{b1, b2}, true, l2)
			if err != nil {
				t.Fatalf("tracker: %v", err)
			}
			if rt.getActiveBaseFolder() != b1 {
				t.Fatalf("expected b1 active")
			}
			ioErr := sop.Error{Code: sop.FailoverQualifiedError + 1, Err: errors.New("io fail")}
			rt.HandleReplicationRelatedError(ctx, ioErr, nil, false)
			if rt.getActiveBaseFolder() != b2 {
				t.Fatalf("expected failover to b2")
			}
			if !rt.FailedToReplicate {
				t.Fatalf("expected FailedToReplicate true")
			}
		}},
		{name: "HandleFailedToReplicate_Idempotent", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			b1 := t.TempDir()
			b2 := t.TempDir()
			GlobalReplicationDetails = nil
			rt, err := NewReplicationTracker(ctx, []string{b1, b2}, true, l2)
			if err != nil {
				t.Fatalf("tracker: %v", err)
			}
			if rt.FailedToReplicate {
				t.Fatalf("should start healthy")
			}
			rt.handleFailedToReplicate(ctx)
			if !rt.FailedToReplicate {
				t.Fatalf("expected failure flag set")
			}
			rt.handleFailedToReplicate(ctx) // no-op
		}},
		{name: "HandleFailedToReplicate_RemoteAlreadyFailed", run: func(t *testing.T) {
			ctx := context.Background()
			cache := mocks.NewMockClient()
			active := filepath.Join(t.TempDir(), "a")
			os.MkdirAll(active, 0o755)
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: true}
			rt, _ := NewReplicationTracker(ctx, []string{active}, true, cache)
			rt.handleFailedToReplicate(ctx)
			if !rt.FailedToReplicate {
				t.Fatalf("expected failure copied")
			}
		}},
		{name: "Failover_GuardBranches", run: func(t *testing.T) {
			ctx := context.Background()
			cache := mocks.NewMockClient()
			a := filepath.Join(t.TempDir(), "a")
			b := filepath.Join(t.TempDir(), "b")
			os.MkdirAll(a, 0o755)
			os.MkdirAll(b, 0o755)
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: false}
			rt, _ := NewReplicationTracker(ctx, []string{a, b}, true, cache)
			rt.ActiveFolderToggler = true
			if err := rt.failover(ctx); err != nil {
				t.Fatalf("guard failover: %v", err)
			}
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
			rt2, _ := NewReplicationTracker(ctx, []string{a, b}, true, cache)
			GlobalReplicationDetails.ActiveFolderToggler = !rt2.ActiveFolderToggler
			if err := rt2.failover(ctx); err != nil {
				t.Fatalf("post-sync guard: %v", err)
			}
		}},
		{name: "FastForward_SkipsStoreReplication_FirstNil", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			active := t.TempDir()
			passive := t.TempDir()
			GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
			rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
			if err != nil {
				t.Fatalf("tracker: %v", err)
			}
			if _, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue); err != nil {
				t.Fatalf("repo init: %v", err)
			}
			payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: nil, Second: [][]sop.RegistryPayload[sop.Handle]{nil, nil, nil, nil}}
			ba, _ := encoding.DefaultMarshaler.Marshal(payload)
			commitDir := rt.formatActiveFolderEntity(commitChangesLogFolder)
			NewFileIO().MkdirAll(ctx, commitDir, 0o755)
			fn := filepath.Join(commitDir, sop.NewUUID().String()+logFileExtension)
			NewFileIO().WriteFile(ctx, fn, ba, permission)
			found, err := rt.fastForward(ctx)
			if err != nil {
				t.Fatalf("fastForward: %v", err)
			}
			if !found {
				t.Fatalf("expected found")
			}
			if NewFileIO().Exists(ctx, filepath.Join(passive, storeListFilename)) {
				t.Fatalf("unexpected passive store list")
			}
			if _, err := os.Stat(fn); err == nil {
				t.Fatalf("log not removed")
			}
		}},
		{name: "ReadStatus_PassiveOnly", run: func(t *testing.T) {
			ctx := context.Background()
			cache := mocks.NewMockClient()
			a := filepath.Join(t.TempDir(), "a")
			p := filepath.Join(t.TempDir(), "b")
			os.MkdirAll(a, 0o755)
			os.MkdirAll(p, 0o755)
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: true}
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, cache)
			rt.ActiveFolderToggler = true
			fnPassive := filepath.Join(p, replicationStatusFilename)
			rt.writeReplicationStatus(ctx, fnPassive)
			os.Remove(filepath.Join(a, replicationStatusFilename))
			if err := rt.readStatusFromHomeFolder(ctx); err != nil {
				t.Fatalf("read: %v", err)
			}
			if rt.ActiveFolderToggler != false {
				t.Fatalf("expected flip")
			}
			if !rt.FailedToReplicate {
				t.Fatalf("expected failure state")
			}
		}},
		{name: "ReadStatus_PassiveNewer_StatError_NoFiles", run: func(t *testing.T) {
			ctx := context.Background()
			cache := mocks.NewMockClient()
			a := filepath.Join(t.TempDir(), "a")
			p := filepath.Join(t.TempDir(), "b")
			os.MkdirAll(a, 0o755)
			os.MkdirAll(p, 0o755)
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
			rt, _ := NewReplicationTracker(ctx, []string{a, p}, true, cache)
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: true}
			rt.writeReplicationStatus(ctx, filepath.Join(p, replicationStatusFilename))
			time.Sleep(10 * time.Millisecond)
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
			rt.writeReplicationStatus(ctx, filepath.Join(a, replicationStatusFilename))
			if err := rt.readStatusFromHomeFolder(ctx); err != nil {
				t.Fatalf("read1: %v", err)
			}
			if rt.ActiveFolderToggler != true {
				t.Fatalf("expected stay active")
			}
			// stat error -> flip
			rt.writeReplicationStatus(ctx, filepath.Join(a, replicationStatusFilename))
			rt.writeReplicationStatus(ctx, filepath.Join(p, replicationStatusFilename))
			os.Remove(filepath.Join(a, replicationStatusFilename))
			if err := rt.readStatusFromHomeFolder(ctx); err != nil {
				t.Fatalf("read2: %v", err)
			}
			if rt.ActiveFolderToggler != false {
				t.Fatalf("expected flip after missing active")
			}
			// no files early return
			os.Remove(filepath.Join(a, replicationStatusFilename))
			os.Remove(filepath.Join(p, replicationStatusFilename))
			rt.ActiveFolderToggler = true
			if err := rt.readStatusFromHomeFolder(ctx); err != nil {
				t.Fatalf("read3: %v", err)
			}
			if rt.ActiveFolderToggler != true {
				t.Fatalf("expected remain true")
			}
		}},
		{name: "ReinstateWorkflowAndFastForwardSingleLog", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			active := t.TempDir()
			passive := t.TempDir()
			GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
			rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
			if err != nil {
				t.Fatalf("tracker: %v", err)
			}
			rt.FailedToReplicate = true
			store := sop.StoreInfo{Name: "s1", Count: 1}
			hNew := sop.NewHandle(sop.NewUUID())
			hAdd := sop.NewHandle(sop.NewUUID())
			hUpd := sop.NewHandle(sop.NewUUID())
			hDel := hAdd
			payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: []sop.StoreInfo{store}, Second: [][]sop.RegistryPayload[sop.Handle]{{{RegistryTable: "reg", IDs: []sop.Handle{hNew}}}, {{RegistryTable: "reg", IDs: []sop.Handle{hAdd}}}, {{RegistryTable: "reg", IDs: []sop.Handle{hUpd}}}, {{RegistryTable: "reg", IDs: []sop.Handle{hDel}}}}}
			ba, _ := encoding.DefaultMarshaler.Marshal(payload)
			commitDir := rt.formatActiveFolderEntity(commitChangesLogFolder)
			os.MkdirAll(commitDir, 0o755)
			logFn := filepath.Join(commitDir, sop.NewUUID().String()+logFileExtension)
			os.WriteFile(logFn, ba, 0o644)
			if err := rt.ReinstateFailedDrives(ctx); err != nil {
				t.Fatalf("ReinstateFailedDrives: %v", err)
			}
			if rt.FailedToReplicate {
				t.Fatalf("expected cleared failure")
			}
			if rt.LogCommitChanges {
				t.Fatalf("expected logging disabled")
			}
			if _, err := os.Stat(logFn); !os.IsNotExist(err) {
				t.Fatalf("log not removed")
			}
			// single log
			GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
			rt2, _ := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
			rt2.FailedToReplicate = true
			store2 := sop.StoreInfo{Name: "s2", Count: 1}
			h := sop.NewHandle(sop.NewUUID())
			payload2 := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: []sop.StoreInfo{store2}, Second: [][]sop.RegistryPayload[sop.Handle]{{{RegistryTable: "reg2", IDs: []sop.Handle{h}}}, {}, {}, {}}}
			ba2, _ := encoding.DefaultMarshaler.Marshal(payload2)
			commitDir2 := rt2.formatActiveFolderEntity(commitChangesLogFolder)
			os.MkdirAll(commitDir2, 0o755)
			logFn2 := filepath.Join(commitDir2, sop.NewUUID().String()+logFileExtension)
			os.WriteFile(logFn2, ba2, 0o644)
			found, err := rt2.fastForward(ctx)
			if err != nil {
				t.Fatalf("fastForward: %v", err)
			}
			if !found {
				t.Fatalf("expected found")
			}
			if _, err := os.Stat(logFn2); !os.IsNotExist(err) {
				t.Fatalf("log2 not removed")
			}
		}},
		{name: "StartLoggingCommitChangesAndSetTransactionID", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			a := t.TempDir()
			p := t.TempDir()
			rt, err := NewReplicationTracker(ctx, []string{a, p}, true, l2)
			if err != nil {
				t.Fatalf("rt: %v", err)
			}
			if err := rt.startLoggingCommitChanges(ctx); err != nil {
				t.Fatalf("startLoggingCommitChanges: %v", err)
			}
			if !rt.LogCommitChanges {
				t.Fatalf("expected LogCommitChanges true")
			}
			id := sop.NewUUID()
			rt.SetTransactionID(id)
		}},
		{name: "AdditionalBranchesAndSyncWithL2Cache", run: func(t *testing.T) {
			ctx := context.Background()
			cache := mocks.NewMockClient()
			a := filepath.Join(t.TempDir(), "a")
			b := filepath.Join(t.TempDir(), "b")
			rtNoRep, _ := NewReplicationTracker(ctx, []string{a}, false, cache)
			rtNoRep.handleFailedToReplicate(ctx)
			rtFail, _ := NewReplicationTracker(ctx, []string{a, b}, true, cache)
			GlobalReplicationDetails = &ReplicationTrackedDetails{FailedToReplicate: true, ActiveFolderToggler: true}
			rtFail.handleFailedToReplicate(ctx)
			if !rtFail.FailedToReplicate {
				t.Fatalf("expected failure copied")
			}
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: false, FailedToReplicate: false}
			rtGuard, _ := NewReplicationTracker(ctx, []string{a, b}, true, cache)
			rtGuard.ActiveFolderToggler = true
			if err := rtGuard.failover(ctx); err != nil {
				t.Fatalf("failover guard: %v", err)
			}
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
			rtDo, _ := NewReplicationTracker(ctx, []string{a, b}, true, cache)
			rtDo.ActiveFolderToggler = true
			os.MkdirAll(a, 0o755)
			_ = rtDo.writeReplicationStatus(ctx, rtDo.formatActiveFolderEntity(replicationStatusFilename))
			ioErr := sop.Error{Code: sop.FailoverQualifiedError, Err: os.ErrInvalid}
			rtDo.HandleReplicationRelatedError(ctx, ioErr, nil, false)
			if !rtDo.FailedToReplicate {
				t.Fatalf("expected failover failure")
			}
			GlobalReplicationDetails = nil
			rtPull, _ := NewReplicationTracker(ctx, []string{a, b}, true, cache)
			if err := rtPull.syncWithL2Cache(ctx, false); err != nil {
				t.Fatalf("pull miss: %v", err)
			}
			if err := rtPull.logCommitChanges(ctx, sop.NewUUID(), nil, nil, nil, nil, nil); err != nil {
				t.Fatalf("logCommitChanges disabled: %v", err)
			}
			GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true, FailedToReplicate: false}
			if err := rtPull.syncWithL2Cache(ctx, true); err != nil {
				t.Fatalf("initial push: %v", err)
			}
			if err := rtPull.syncWithL2Cache(ctx, true); err != nil {
				t.Fatalf("push identical: %v", err)
			}
			GlobalReplicationDetails.FailedToReplicate = true
			if err := rtPull.syncWithL2Cache(ctx, true); err != nil {
				t.Fatalf("push diverged: %v", err)
			}
		}},
		{name: "HandleFailedToReplicate_WriteStatusFail", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			a := t.TempDir()
			p := t.TempDir()
			GlobalReplicationDetails = nil
			rt, err := NewReplicationTracker(ctx, []string{a, p}, true, l2)
			if err != nil {
				t.Fatalf("tracker: %v", err)
			}
			statusPath := rt.formatActiveFolderEntity(replicationStatusFilename)
			os.MkdirAll(statusPath, 0o755)
			rt.handleFailedToReplicate(ctx)
			if !rt.FailedToReplicate {
				t.Fatalf("expected failure flag true")
			}
			os.RemoveAll(filepath.Join(a, replicationStatusFilename))
		}},
		{name: "StatusWriteAndReadErrors", run: func(t *testing.T) {
			if runtime.GOOS == "windows" {
				t.Skip("windows path semantics")
			}
			ctx := context.Background()
			active := t.TempDir()
			passive := t.TempDir()
			l2 := mocks.NewMockClient()
			rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
			if err != nil {
				t.Fatalf("tracker: %v", err)
			}
			statusPath := rt.formatActiveFolderEntity(replicationStatusFilename)
			os.Mkdir(statusPath, 0o755)
			if err := rt.writeReplicationStatus(ctx, statusPath); err == nil {
				t.Fatalf("expected write error")
			}
			os.Remove(statusPath)
			os.WriteFile(statusPath, []byte("{malformed"), 0o644)
			if err := rt.readReplicationStatus(ctx, statusPath); err == nil {
				t.Fatalf("expected malformed read error")
			}
		}},
		{name: "FastForwardProcessesLogs", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			active := t.TempDir()
			passive := t.TempDir()
			GlobalReplicationDetails = nil
			rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
			if err != nil {
				t.Fatalf("NewReplicationTracker: %v", err)
			}
			if _, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue); err != nil {
				t.Fatalf("NewStoreRepository: %v", err)
			}
			sr2, _ := NewStoreRepository(ctx, rt, nil, l2, 0)
			s := sop.NewStoreInfo(sop.StoreOptions{Name: "s2", SlotLength: 8})
			if err := sr2.Add(ctx, *s); err != nil {
				t.Fatalf("Add: %v", err)
			}
			tid := sop.NewUUID()
			payload := sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{First: []sop.StoreInfo{*s}, Second: [][]sop.RegistryPayload[sop.Handle]{{}, {}, {}, {}}}
			ba, _ := encoding.DefaultMarshaler.Marshal(payload)
			fn := rt.formatActiveFolderEntity(filepath.Join(commitChangesLogFolder, tid.String()+logFileExtension))
			if err := NewFileIO().WriteFile(ctx, fn, ba, permission); err != nil {
				t.Fatalf("write commit log: %v", err)
			}
			found, err := rt.fastForward(ctx)
			if err != nil {
				t.Fatalf("fastForward: %v", err)
			}
			if !found {
				t.Fatalf("expected found log")
			}
			found, err = rt.fastForward(ctx)
			if err != nil {
				t.Fatalf("fastForward 2: %v", err)
			}
			if found {
				t.Fatalf("expected no logs")
			}
		}},
		{name: "TurnOnReplicationUpdatesStatus", run: func(t *testing.T) {
			ctx := context.Background()
			l2 := mocks.NewMockClient()
			active := t.TempDir()
			passive := t.TempDir()
			GlobalReplicationDetails = nil
			rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, l2)
			if err != nil {
				t.Fatalf("NewReplicationTracker: %v", err)
			}
			GlobalReplicationDetails.FailedToReplicate = true
			GlobalReplicationDetails.LogCommitChanges = true
			if err := rt.turnOnReplication(ctx); err != nil {
				t.Fatalf("turnOnReplication: %v", err)
			}
			if GlobalReplicationDetails.FailedToReplicate || GlobalReplicationDetails.LogCommitChanges {
				t.Fatalf("expected flags cleared; got %+v", GlobalReplicationDetails)
			}
			if !NewFileIO().Exists(ctx, rt.formatActiveFolderEntity(replicationStatusFilename)) {
				t.Fatalf("expected status file")
			}
		}},
	}
	for _, sc := range scenarios {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			prev := GlobalReplicationDetails
			defer func() { GlobalReplicationDetails = prev }()
			GlobalReplicationDetails = nil
			sc.run(t)
		})
	}
}
