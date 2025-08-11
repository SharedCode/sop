package fs

import (
    "testing"

    "github.com/sharedcode/sop"
)

func TestGetIDsAndOffsets(t *testing.T) {
    ids := []sop.Handle{
        sop.NewHandle(uuid),
    }
    got := getIDs(ids)
    if len(got) != 1 || got[0] != ids[0].LogicalID {
        t.Fatalf("getIDs mismatch: %+v vs %+v", got, ids)
    }

    hm := newHashmap(true, 10, nil, nil)
    high, low := uuid.Split()
    bo, io := hm.getBlockOffsetAndHandleInBlockOffset(uuid)
    if bo < 0 || io < 0 {
        t.Fatalf("negative offsets: bo=%d io=%d", bo, io)
    }
    // Basic derivation checks: mod relations hold.
    if int64((high%uint64(hm.hashModValue))*blockSize) != bo {
        t.Errorf("block offset mismatch: got %d", bo)
    }
    if int64((low%uint64(handlesPerBlock))*sop.HandleSizeInBytes) != io {
        t.Errorf("in-block offset mismatch: got %d", io)
    }
}
