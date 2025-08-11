package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"
    "time"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
    "github.com/sharedcode/sop/encoding"
)

func TestPriorityLogBasics(t *testing.T) {
    ctx := context.Background()
    base := filepath.Join(t.TempDir(), "a")
    rt, err := NewReplicationTracker(ctx, []string{base, filepath.Join(t.TempDir(), "b")}, false, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    tl := NewTransactionLog(mocks.NewMockClient(), rt)
    plog := tl.PriorityLog()

    if !plog.IsEnabled() {
        t.Fatalf("IsEnabled expected true")
    }

    // Add and Get
    tid := sop.NewUUID()
    payload := []sop.RegistryPayload[sop.Handle]{
        {RegistryTable: "rt1", BlobTable: "bt1", IDs: []sop.Handle{{LogicalID: sop.NewUUID()}}},
    }
    ba, _ := encoding.DefaultMarshaler.Marshal(payload)
    if err := plog.Add(ctx, tid, ba); err != nil { t.Fatalf("Add: %v", err) }
    got, err := plog.Get(ctx, tid)
    if err != nil { t.Fatalf("Get: %v", err) }
    if len(got) != 1 || got[0].RegistryTable != "rt1" { t.Fatalf("Get payload mismatch: %+v", got) }

    // WriteBackup and RemoveBackup are best-effort; just invoke them
    if err := plog.WriteBackup(ctx, tid, ba); err != nil { t.Fatalf("WriteBackup: %v", err) }
    if err := plog.RemoveBackup(ctx, tid); err != nil { t.Fatalf("RemoveBackup: %v", err) }

    // Remove
    if err := plog.Remove(ctx, tid); err != nil { t.Fatalf("Remove: %v", err) }
}

func TestPriorityLogGetBatchOlderHour(t *testing.T) {
    ctx := context.Background()
    base := filepath.Join(t.TempDir(), "a")
    rt, err := NewReplicationTracker(ctx, []string{base, filepath.Join(t.TempDir(), "b")}, false, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    tl := NewTransactionLog(mocks.NewMockClient(), rt)
    plog := tl.PriorityLog()

    // Create two priority log files and age them to previous hour so they are eligible
    tids := []sop.UUID{sop.NewUUID(), sop.NewUUID()}
    for _, id := range tids {
        ba, _ := encoding.DefaultMarshaler.Marshal([]sop.RegistryPayload[sop.Handle]{
            {RegistryTable: "rt2", IDs: []sop.Handle{{LogicalID: sop.NewUUID()}}},
        })
        if err := plog.Add(ctx, id, ba); err != nil { t.Fatalf("Add: %v", err) }
        fn := rt.formatActiveFolderEntity(filepath.Join(logFolder, id.String()+priorityLogFileExtension))
        // age to previous hour
        past := time.Now().Add(-65 * time.Minute)
        if err := os.Chtimes(fn, past, past); err != nil { t.Fatalf("chtimes: %v", err) }
    }

    batch, err := plog.GetBatch(ctx, 10)
    if err != nil { t.Fatalf("GetBatch: %v", err) }
    if len(batch) != 2 { t.Fatalf("expected 2, got %d", len(batch)) }
    if batch[0].Value[0].RegistryTable != "rt2" { t.Fatalf("payload mismatch: %+v", batch[0]) }
}

func TestPriorityLogLogCommitChangesWritesWhenEnabled(t *testing.T) {
    ctx := context.Background()
    base := filepath.Join(t.TempDir(), "a")
    rt, err := NewReplicationTracker(ctx, []string{base, filepath.Join(t.TempDir(), "b")}, false, mocks.NewMockClient())
    if err != nil { t.Fatalf("rt: %v", err) }
    rt.LogCommitChanges = true
    tl := NewTransactionLog(mocks.NewMockClient(), rt)
    // ensure base exists
    if err := os.MkdirAll(base, 0o755); err != nil { t.Fatalf("mkdir: %v", err) }
    // use PriorityLog passthrough
    tid := sop.NewUUID()
    tl.tid = tid
    if err := tl.PriorityLog().LogCommitChanges(ctx, nil, nil, nil, nil, nil); err != nil {
        t.Fatalf("LogCommitChanges: %v", err)
    }
    fn := rt.formatActiveFolderEntity(filepath.Join(commitChangesLogFolder, tid.String()+logFileExtension))
    if _, err := os.Stat(fn); err != nil { t.Fatalf("commit log missing: %v", err) }
}
