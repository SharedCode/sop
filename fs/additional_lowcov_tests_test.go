package fs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

func TestHashmap_Fetch_MixedIDs(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rt, _ := NewReplicationTracker(ctx, []string{base}, false, mocks.NewMockClient())
	hm := newHashmap(true, 32, rt, mocks.NewMockClient())
	idExisting := sop.NewUUID()
	frd, err := hm.findOneFileRegion(ctx, true, "tblmix", idExisting)
	if err != nil {
		t.Fatalf("prep frd: %v", err)
	}
	frd.handle = sop.NewHandle(idExisting)
	if err := hm.updateFileRegion(ctx, []fileRegionDetails{frd}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	missing := sop.NewUUID()
	r, err := hm.fetch(ctx, "tblmix", []sop.UUID{idExisting, missing})
	if err != nil || len(r) != 1 || r[0].LogicalID != idExisting {
		t.Fatalf("unexpected fetch result: %v %+v", err, r)
	}
}

func TestReplicationTracker_Failover_NoOp_RollbackSucceeded(t *testing.T) {
	ctx := context.Background()
	a := t.TempDir()
	b := t.TempDir()
	GlobalReplicationDetails = &ReplicationTrackedDetails{ActiveFolderToggler: true}
	rt, _ := NewReplicationTracker(ctx, []string{a, b}, true, mocks.NewMockClient())
	before := rt.ActiveFolderToggler
	ioErr := sop.Error{Code: sop.FileIOError, Err: errors.New("io temp")}
	rt.HandleReplicationRelatedError(ctx, ioErr, nil, true)
	if rt.ActiveFolderToggler != before || rt.FailedToReplicate {
		t.Fatalf("expected no failover; state changed: toggler %v->%v failed=%v", before, rt.ActiveFolderToggler, rt.FailedToReplicate)
	}
	rt.FailedToReplicate = true
	rt.handleFailedToReplicate(ctx)
	_ = time.Second
}
