package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSafeOpenContained_RejectsTraversal is the regression test for the
// arbitrary file read finding. Before this fix, ingestImportReader called
// os.Open on a client-supplied PreloadFilePath with no containment,
// allowing any authenticated user to read arbitrary files from the
// server's filesystem.
func TestSafeOpenContained_RejectsTraversal(t *testing.T) {
	root := t.TempDir()

	// Create a file inside the allowed root so we know it exists.
	allowed := filepath.Join(root, "ok.json")
	if err := os.WriteFile(allowed, []byte("{}"), 0644); err != nil {
		t.Fatal(err)
	}

	// A file outside the root must be rejected even though it exists.
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0644); err != nil {
		t.Fatal(err)
	}

	// The allowed file opens fine.
	f, err := safeOpenContained(allowed, []string{root})
	if err != nil {
		t.Fatalf("expected allowed path to succeed, got: %v", err)
	}
	f.Close()

	// A path outside the root must fail.
	if f, err := safeOpenContained(outside, []string{root}); err == nil {
		f.Close()
		t.Fatal("expected path outside root to be rejected, but it was allowed")
	}

	// A relative traversal that escapes the root must fail. The traversal
	// path is crafted so it resolves to the outside file's absolute path.
	rel, err := filepath.Rel(root, outside)
	if err != nil {
		t.Fatalf("filepath.Rel: %v", err)
	}
	traversal := filepath.Join(root, rel)
	if f, err := safeOpenContained(traversal, []string{root}); err == nil {
		f.Close()
		t.Fatalf("expected traversal path %q to be rejected, but it was allowed", traversal)
	}
}

// TestSafeOpenContained_RejectsAbsoluteEscape confirms that passing an
// absolute path like "/etc/passwd" is rejected when it falls outside all
// allowed roots.
func TestSafeOpenContained_RejectsAbsoluteEscape(t *testing.T) {
	root := t.TempDir()

	_, err := safeOpenContained("/etc/passwd", []string{root})
	if err == nil {
		t.Fatal("expected /etc/passwd to be rejected when root is a temp dir")
	}
}

// TestSafeOpenContained_PrefixConfusion confirms that a directory whose
// name starts with the allowed root's name (e.g. /tmp/sop vs /tmp/sopevil)
// is not mistakenly allowed.
func TestSafeOpenContained_PrefixConfusion(t *testing.T) {
	// Create two sibling directories where one name is a prefix of the other.
	parent := t.TempDir()
	allowed := filepath.Join(parent, "data")
	sibling := filepath.Join(parent, "dataevil")
	os.MkdirAll(allowed, 0755)
	os.MkdirAll(sibling, 0755)

	outsideFile := filepath.Join(sibling, "steal.txt")
	if err := os.WriteFile(outsideFile, []byte("stolen"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := safeOpenContained(outsideFile, []string{allowed})
	if err == nil {
		t.Fatal("expected prefix-confused path to be rejected")
	}
}
