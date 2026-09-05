package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// safeOpenContained resolves path to an absolute path and verifies that
// the result starts with one of the allowed root directories before
// opening the file. This prevents a client-supplied path from reading
// arbitrary files on the system (e.g. "/etc/shadow" or
// "../../../.ssh/authorized_keys").
//
// allowedRoots must already be absolute, clean paths.
func safeOpenContained(path string, allowedRoots []string) (*os.File, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("cannot resolve path: %w", err)
	}
	abs = filepath.Clean(abs)

	for _, root := range allowedRoots {
		// filepath.Rel can resolve a relative path even for unrelated
		// directories. Instead check the prefix directly after cleaning.
		// Append os.PathSeparator to avoid "/tmp/sopevil" matching "/tmp/sop".
		if strings.HasPrefix(abs, root+string(os.PathSeparator)) || abs == root {
			return os.Open(abs)
		}
	}
	return nil, fmt.Errorf("path %q is outside all allowed directories", path)
}

// preloadAllowedRoots returns the set of directories that file-path
// arguments to the ingest/import handlers are allowed to reference. This
// includes the server's working directory (where the builtin JSON
// templates live) and every configured database path, since those are
// the only locations the server has a reason to read from.
func preloadAllowedRoots() []string {
	var roots []string
	if cwd, err := os.Getwd(); err == nil {
		roots = append(roots, filepath.Clean(cwd))
	}
	for _, db := range config.Databases {
		if abs, err := filepath.Abs(db.Path); err == nil {
			roots = append(roots, filepath.Clean(abs))
		}
	}
	if config.SystemDB != nil {
		if abs, err := filepath.Abs(config.SystemDB.Path); err == nil {
			roots = append(roots, filepath.Clean(abs))
		}
	}
	return roots
}
