package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestFileIOBasicScenarios exercises the default FileIO implementation across
// write (both direct success and mkdir+retry branch), read, exists, read dir,
// remove, mkdir/removeall flows. Table form keeps file near size guidance.
func TestFileIOBasicScenarios(t *testing.T) {
	ctx := context.Background()
	fio := NewFileIO()
	base := t.TempDir()

	type writeCase struct {
		name        string
		relPath     string
		parentFirst bool // if true, create parent beforehand to hit immediate success path
	}

	cases := []writeCase{
		{name: "mkdir_retry_branch", relPath: filepath.Join("nested1", "a", "file.txt")},
		{name: "direct_success", relPath: filepath.Join("nested2", "file.txt"), parentFirst: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			target := filepath.Join(base, c.relPath)
			if c.parentFirst {
				if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
					t.Fatalf("pre mkdir: %v", err)
				}
			}
			content := []byte("hello-" + c.name)
			if err := fio.WriteFile(ctx, target, content, 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			if !fio.Exists(ctx, target) {
				t.Fatalf("expected exists after write")
			}
			// Read back
			rb, err := fio.ReadFile(ctx, target)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if string(rb) != string(content) {
				t.Fatalf("content mismatch got=%q want=%q", rb, content)
			}
		})
	}

	// Verify directory listing includes written files.
	entries, err := fio.ReadDir(ctx, filepath.Join(base, "nested2"))
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected entries in nested2")
	}

	// Remove a file and ensure no longer exists.
	toRemove := filepath.Join(base, cases[0].relPath)
	if err := fio.Remove(ctx, toRemove); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if fio.Exists(ctx, toRemove) {
		t.Fatalf("expected removed file to not exist")
	}

	// MkdirAll + RemoveAll round trip for additional paths.
	dirPath := filepath.Join(base, "tempdir", "child")
	if err := fio.MkdirAll(ctx, dirPath, 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	if !fio.Exists(ctx, dirPath) {
		t.Fatalf("expected dir exists")
	}
	if err := fio.RemoveAll(ctx, filepath.Join(base, "tempdir")); err != nil {
		t.Fatalf("removeall: %v", err)
	}
	if fio.Exists(ctx, dirPath) {
		t.Fatalf("expected dir removed")
	}
}
