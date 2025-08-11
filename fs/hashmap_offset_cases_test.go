package fs

import "testing"

func TestFileRegionDetailsGetOffset(t *testing.T) {
    fr := fileRegionDetails{blockOffset: 4096, handleInBlockOffset: 128}
    if got := fr.getOffset(); got != 4224 {
        t.Fatalf("getOffset: want 4224, got %d", got)
    }
}
