package fs

import (
	"fmt"
	"os"

	"github.com/SharedCode/sop"
)

type ToFilePathFunc func(basePath string, id sop.UUID) string

// ToFilePath is used for formatting a base folder path & a GUID into a file path & file name
// optimized for efficient I/O. It can be a simple logic as the default function shown below
// or you can implement as fancy as partitioning across many storage devices, e.g. - using
// the 1st hex digit, apply modulo to distribute to your different storage devices.
//
// Or using the basePath to specify different storage path, this perhaps is the typical case(default).
var ToFilePath ToFilePathFunc = DefaultToFilePath

// Default file path formatter, given a base path & a GUID.
func DefaultToFilePath(basePath string, id sop.UUID) string {
	return fmt.Sprintf("%s%c%s", basePath, os.PathSeparator, Apply3LevelHierarchyAndModulo(id))
}

// Support 3 level folders and 4th hex "modulo" file distribution algorithm.
func Apply3LevelHierarchyAndModulo(id sop.UUID) string {
	s := id.String()
	ps := os.PathSeparator
	mod := s[3] % 3
	return fmt.Sprintf("%c%c%c%c%c%c%c", s[0], ps, s[1], ps, s[2], ps, mod)
}
