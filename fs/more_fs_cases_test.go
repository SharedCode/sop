package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers fileIO.read path through FileIOWithReplication wrapper.
func TestFileIOWithReplicationRead(t *testing.T) {
	ctx := context.Background()
	active := filepath.Join(t.TempDir(), "a")
	passive := filepath.Join(t.TempDir(), "p")
	rt, _ := NewReplicationTracker(ctx, []string{active, passive}, true, mocks.NewMockClient())
	ms := NewManageStoreFolder(nil)
	fio := newFileIOWithReplication(rt, ms, false)

	name := "foo.data"
	full := rt.formatActiveFolderEntity(name)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte("abc"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := fio.read(ctx, name)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "abc" {
		t.Fatalf("bad read: %q", string(got))
	}
}

// Covers fileIOSimulator ToFilePath/Exists/ReadDir/MkdirAll/RemoveAll no-op behaviors.
func TestFileIOSimulatorBasics(t *testing.T) {
	sim := newFileIOSim()
	ctx := context.Background()
	if sim.ToFilePath("base", sop.NewUUID()) != "" {
		t.Fatalf("ToFilePath expected empty string")
	}
	if !sim.Exists(ctx, "any") {
		t.Fatalf("Exists expected true")
	}
	if err := sim.MkdirAll(ctx, "x", 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := sim.RemoveAll(ctx, "x"); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if _, err := sim.ReadDir(ctx, "x"); err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
}

// Consolidated: fileIOSimulator error injection (write/read/remove) and reset flag behaviors.
func TestFileIOSimulator_ErrorInjectionAndResetScenarios(t *testing.T) {
	sim := newFileIOSim()
	ctx := context.Background()
	// Induce write error on suffix _1
	sim.setErrorOnSuffixNumber(1)
	if err := sim.WriteFile(ctx, "file_1", []byte("x"), 0o644); err == nil {
		t.Fatalf("expected write error")
	}
	// Write different suffix ok
	if err := sim.WriteFile(ctx, "file_2", []byte("ok"), 0o644); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	if _, err := sim.ReadFile(ctx, "file_2"); err != nil {
		t.Fatalf("initial read: %v", err)
	}
	// Induce read error w/ auto-reset on suffix _2 (second flag)
	sim.setResetFlag(true)
	sim.setErrorOnSuffixNumber2(2)
	if _, err := sim.ReadFile(ctx, "file_2"); err == nil {
		t.Fatalf("expected induced read error")
	}
	if b, err := sim.ReadFile(ctx, "file_2"); err != nil || string(b) != "ok" {
		t.Fatalf("post-reset read: %v %q", err, string(b))
	}
	// Remove error injection via write flag then normal remove
	sim.setErrorOnSuffixNumber(2)
	if err := sim.Remove(ctx, "file_2"); err == nil {
		t.Fatalf("expected induced remove err")
	}
	sim.setErrorOnSuffixNumber(-1)
	if err := sim.Remove(ctx, "file_2"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	// Reset flag path using suffix _0 via second flag
	name := "dummy_0"
	if err := sim.WriteFile(ctx, name, []byte("ok"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sim.setResetFlag(true)
	sim.setErrorOnSuffixNumber2(0)
	if _, err := sim.ReadFile(ctx, name); err == nil {
		t.Fatalf("expected read err for suffix 0")
	}
	if b, err := sim.ReadFile(ctx, name); err != nil || string(b) != "ok" {
		t.Fatalf("second read expected ok: %v %q", err, string(b))
	}
}

// ManageStoreFolder basic create/remove lifecycle (migrated from managestorefolder_cases_test.go).
func TestManageStoreFolder_CreateRemove(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "stores", "a", "b")
	ms := NewManageStoreFolder(nil)
	if err := ms.CreateStore(ctx, base); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	if _, err := os.Stat(base); err != nil {
		t.Fatalf("expected folder exists: %v", err)
	}
	if err := ms.RemoveStore(ctx, filepath.Dir(base)); err != nil {
		t.Fatalf("RemoveStore: %v", err)
	}
}

// retryIO success after transient errors (migrated from retryio_cases_test.go).
func TestRetryIO_RetryableThenSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var attempts int32
	err := retryIO(ctx, func(context.Context) error {
		a := atomic.AddInt32(&attempts, 1)
		if a <= 2 {
			return sop.Error{Code: sop.FileIOError, Err: errors.New("transient")}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("retryIO unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

// Path building helpers (from tofilepath_more_cases_test.go).
func TestApply4LevelHierarchy(t *testing.T) {
	id := sop.UUID{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90}
	got := Apply4LevelHierarchy(id)
	sep := string(os.PathSeparator)
	parts := strings.Split(got, sep)
	if len(parts) != 4 {
		t.Fatalf("expected 4 parts, got %d: %q", len(parts), got)
	}
	if parts[0] != "a" || parts[1] != "b" || parts[2] != "c" || parts[3] != "d" {
		t.Fatalf("expected a/b/c/d, got %q", got)
	}
}
func TestDefaultToFilePath_Variants(t *testing.T) {
	sep := string(os.PathSeparator)
	id := sop.UUID{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90}
	cases := []string{"", filepath.Join("a", "b"), filepath.Join("a", "b") + sep, filepath.Join("a", "b") + sep + sep}
	for _, base := range cases {
		got := DefaultToFilePath(base, id)
		if !strings.Contains(got, sep+"a"+sep+"b"+sep+"c"+sep+"d") {
			t.Fatalf("missing hierarchy in %q", got)
		}
	}
}
func TestToFilePath_OverrideAndRestore(t *testing.T) {
	old := ToFilePath
	t.Cleanup(func() { ToFilePath = old })
	id := sop.NewUUID()
	ToFilePath = func(base string, id sop.UUID) string { return "X:" + base + ":" + id.String() }
	if !strings.HasPrefix(ToFilePath("base", id), "X:base:") {
		t.Fatalf("override not effective")
	}
}

// Documentation-only unreachable mismatch branches (from registrymap_mismatch_unreachable_cases_test.go).
func TestRegistryMapMismatchUnreachable(t *testing.T) {
	t.Skip("registryMap mismatch branches currently unreachable; see hashing logic")
}

// Exercise replicationTracker.readStatusFromHomeFolder and handleFailedToReplicate branches.
func TestReplicationTrackerReadStatusAndFailureFlag(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	a := filepath.Join(base, "a")
	b := filepath.Join(base, "b")
	// Write status only in passive to force toggling when reading.
	rt, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	rt.ActiveFolderToggler = false // make b active initially for status write
	if err := os.MkdirAll(b, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := rt.writeReplicationStatus(ctx, rt.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
		t.Fatalf("write: %v", err)
	}

	// New tracker should flip to b when reading status from home folder
	rt2, err := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt2: %v", err)
	}
	// Simulate a failure and ensure handleFailedToReplicate sets flags and writes status in active
	rt2.FailedToReplicate = false
	rt2.handleFailedToReplicate(ctx)
	if !rt2.FailedToReplicate {
		t.Fatalf("expected FailedToReplicate true")
	}
	fn := rt2.formatActiveFolderEntity(replicationStatusFilename)
	if _, err := os.Stat(fn); err != nil {
		t.Fatalf("status exists: %v", err)
	}
	// Age files to ensure coverage for date comparisons
	past := time.Now().Add(-10 * time.Minute)
	os.Chtimes(fn, past, past)
}
