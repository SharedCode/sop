package fs

import (
    "context"
    "os"
    "path/filepath"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/common/mocks"
)

// Exercises Update happy path for first item and forced failure on second item to trigger undo.
func TestStoreRepositoryUpdateUndoOnSecondFailure(t *testing.T) {
    ctx := context.Background()
    l2 := mocks.NewMockClient()
    base := t.TempDir()
    rt, _ := NewReplicationTracker(ctx, []string{base}, false, l2)
    sr, err := NewStoreRepository(ctx, rt, nil, l2, MinimumModValue)
    if err != nil { t.Fatalf("NewStoreRepository: %v", err) }

    s1 := sop.NewStoreInfo(sop.StoreOptions{Name: "aaa", SlotLength: 10})
    s2 := sop.NewStoreInfo(sop.StoreOptions{Name: "bbb", SlotLength: 10})
    if err := sr.Add(ctx, *s1, *s2); err != nil { t.Fatalf("Add: %v", err) }

    // Prepare updates; ensure the storeinfo file for s2 is read-only to force write failure mid-flight
    upd1 := *s1; upd1.CountDelta = 3
    upd2 := *s2; upd2.CountDelta = 5
    s2File := filepath.Join(base, upd2.Name, "storeinfo.txt")
    if err := os.Chmod(s2File, 0o444); err != nil { t.Fatalf("chmod ro file: %v", err) }
    defer os.Chmod(s2File, 0o644)

    // Expect error due to s2 write failure and undo to restore s1
    if _, err := sr.Update(ctx, []sop.StoreInfo{upd1, upd2}); err == nil {
        t.Fatalf("expected Update error due to second store write failure")
    }

    // s1 should be restored back (Count remains zero)
    got, err := sr.Get(ctx, s1.Name, s2.Name)
    if err != nil { t.Fatalf("Get: %v", err) }
    var g1, g2 sop.StoreInfo
    for _, v := range got { if v.Name == s1.Name { g1 = v } else if v.Name == s2.Name { g2 = v } }
    if g1.Count != 0 { t.Fatalf("undo expected Count=0 for %s, got %d", g1.Name, g1.Count) }
    if g2.Count != 0 { t.Fatalf("%s should remain unchanged Count=0, got %d", g2.Name, g2.Count) }
}
