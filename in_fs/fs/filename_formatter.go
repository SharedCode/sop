package fs

import "github.com/SharedCode/sop"

// TODO: support 3 level folders and 4th hex "modulo".

type ToFilePathFunc func(basePath string, id sop.UUID) string

// Filename formatter is used for formatting a GUID into a file path & file name
// optimized for efficient I/O.
var FilenameFormatter ToFilePathFunc = DefaultToFilePath

// Default file path formatter, given a GUID.
func DefaultToFilePath(basePath string, id sop.UUID) string {
	return ""
}
