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

// TestPriorityLogAdditionalBranches targets remaining uncovered branches: Get missing file (nil,nil), Get unmarshal error,
// GetBatch with no eligible files, and malformed filename skip.
func TestPriorityLogAdditionalBranches(t *testing.T) {
	ctx := context.Background()
	base := filepath.Join(t.TempDir(), "a")
	rt, err := NewReplicationTracker(ctx, []string{base, filepath.Join(t.TempDir(), "b")}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)
	plog := tl.PriorityLog()

	if r, e := plog.Get(ctx, sop.NewUUID()); e != nil || r != nil {
		t.Fatalf("expected nil,nil for missing file, got %v,%v", r, e)
	}

	tid := sop.NewUUID()
	fn := rt.formatActiveFolderEntity(filepath.Join(logFolder, tid.String()+priorityLogFileExtension))
	if err := os.MkdirAll(filepath.Dir(fn), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(fn, []byte("not-json"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if r, e := plog.Get(ctx, tid); e == nil || r != nil {
		t.Fatalf("expected unmarshal error, got r=%v err=%v", r, e)
	}

	tid2 := sop.NewUUID()
	fn2 := rt.formatActiveFolderEntity(filepath.Join(logFolder, tid2.String()+priorityLogFileExtension))
	if err := os.WriteFile(fn2, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write2: %v", err)
	}
	if batch, e := plog.GetBatch(ctx, 10); e != nil || len(batch) != 0 {
		t.Fatalf("expected empty slice (no eligible), got %v err=%v", batch, e)
	}

	badfn := rt.formatActiveFolderEntity(filepath.Join(logFolder, "badname"+priorityLogFileExtension))
	if err := os.WriteFile(badfn, []byte("[]"), 0o644); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	if batch, e := plog.GetBatch(ctx, 0); e != nil || len(batch) != 0 {
		t.Fatalf("expected empty slice after malformed, got %v err=%v", batch, e)
	}
}

// failingCloseDirectIO covers fileDirectIO.close error propagation.
type failingCloseDirectIO struct{ DirectIO }

func (f failingCloseDirectIO) Close(file *os.File) error { return errors.New("close fail") }
func (f failingCloseDirectIO) Open(ctx context.Context, filename string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filename, flag|os.O_CREATE, perm)
}
func (f failingCloseDirectIO) WriteAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.WriteAt(block, offset)
}
func (f failingCloseDirectIO) ReadAt(ctx context.Context, file *os.File, block []byte, offset int64) (int, error) {
	return file.ReadAt(block, offset)
}

func TestFileDirectIOCloseError(t *testing.T) {
	ctx := context.Background()
	prev := DirectIOSim
	DirectIOSim = failingCloseDirectIO{}
	defer func() { DirectIOSim = prev }()
	fio := newFileDirectIO()
	name := filepath.Join(t.TempDir(), "c.dat")
	if err := fio.open(ctx, name, os.O_RDWR, 0o644); err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := fio.close(); err == nil || err.Error() != "close fail" {
		t.Fatalf("expected close fail error, got %v", err)
	}
}
