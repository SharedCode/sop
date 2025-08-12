package fs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
)

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

func TestDefaultToFilePath_AppendsSeparatorWhenMissing(t *testing.T) {
	id := sop.UUID{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90}
	base := filepath.Join("tmp", "base")
	got := DefaultToFilePath(base, id)
	wantPrefix := base + string(os.PathSeparator) + "a" + string(os.PathSeparator) + "b" + string(os.PathSeparator) + "c" + string(os.PathSeparator) + "d"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("expected path to start with %q, got %q", wantPrefix, got)
	}
}

func TestDefaultToFilePath_PreservesTrailingSeparator(t *testing.T) {
	id := sop.UUID{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90}
	base := filepath.Join("tmp", "base") + string(os.PathSeparator)
	got := DefaultToFilePath(base, id)
	wantPrefix := base + "a" + string(os.PathSeparator) + "b" + string(os.PathSeparator) + "c" + string(os.PathSeparator) + "d"
	if !strings.HasPrefix(got, wantPrefix) {
		t.Fatalf("expected path to start with %q, got %q", wantPrefix, got)
	}
}

// Table-driven coverage for multiple basePath variants.
func TestDefaultToFilePath_Table(t *testing.T) {
	sep := string(os.PathSeparator)
	id := sop.UUID{0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90}
	cases := []struct {
		name  string
		base  string
		wantP string // expected prefix
	}{
		{
			name:  "no-trailing-sep",
			base:  filepath.Join("base", "path"),
			wantP: filepath.Join("base", "path") + sep + "a" + sep + "b" + sep + "c" + sep + "d",
		},
		{
			name:  "with-trailing-sep",
			base:  filepath.Join("base", "path") + sep,
			wantP: filepath.Join("base", "path") + sep + "a" + sep + "b" + sep + "c" + sep + "d",
		},
		{
			name:  "empty-base",
			base:  "",
			wantP: sep + "a" + sep + "b" + sep + "c" + sep + "d",
		},
		{
			name:  "double-trailing-sep",
			base:  filepath.Join("base", "path") + sep + sep,
			wantP: filepath.Join("base", "path") + sep + sep + "a" + sep + "b" + sep + "c" + sep + "d",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := DefaultToFilePath(tc.base, id)
			if !strings.HasPrefix(got, tc.wantP) {
				t.Fatalf("%s: expected prefix %q, got %q", tc.name, tc.wantP, got)
			}
		})
	}
}

func TestToFilePath_OverrideAndRestore(t *testing.T) {
	// Save and restore the global to avoid test interference.
	old := ToFilePath
	t.Cleanup(func() { ToFilePath = old })

	id := sop.UUID{0xde, 0xad, 0xbe, 0xef, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0xaa, 0xbb}
	ToFilePath = func(base string, id sop.UUID) string {
		return "X:" + base + ":" + id.String()
	}
	got := ToFilePath("base", id)
	if !strings.HasPrefix(got, "X:base:") {
		t.Fatalf("override not effective, got %q", got)
	}
}
