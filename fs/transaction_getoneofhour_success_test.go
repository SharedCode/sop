package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers the successful GetOneOfHour path (eligible file within TTL window returning records).
func TestTransactionLog_GetOneOfHour_Success(t *testing.T) {
	ctx := context.Background()
	rt, err := NewReplicationTracker(ctx, []string{t.TempDir(), t.TempDir()}, false, mocks.NewMockClient())
	if err != nil {
		t.Fatalf("rt: %v", err)
	}
	tl := NewTransactionLog(mocks.NewMockClient(), rt)

	tid := sop.NewUUID()
	// Add a few records (creates the file and leaves it open).
	for i := 0; i < 3; i++ {
		if err := tl.Add(ctx, tid, i, []byte{byte('a' + i)}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	// Age the file so it becomes eligible per getOne filter (>= ageLimit minutes old at the hour granularity).
	fn := rt.formatActiveFolderEntity(filepath.Join(logFolder, tid.String()+logFileExtension))
	past := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(fn, past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// Provide current hour (any hour within 4h window is fine).
	hour := time.Now().Format(DateHourLayout)
	gotTid, recs, err := tl.GetOneOfHour(ctx, hour)
	if err != nil {
		t.Fatalf("GetOneOfHour err: %v", err)
	}
	if gotTid.IsNil() || gotTid != tid {
		t.Fatalf("expected tid %s got %s", tid, gotTid)
	}
	if len(recs) == 0 {
		t.Fatalf("expected records, got 0")
	}
}
