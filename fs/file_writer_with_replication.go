package fs

import "github.com/SharedCode/sop"

type fileWriter struct {
	filenames            []string
	tempFilenames        []string
	tempNameSuffix       string
	replicate            bool
	replicateToFilenames []string
	cache sop.Cache
}

func newFileWriterWithReplication(replicate bool, cache sop.Cache) *fileWriter {
	return &fileWriter{
		tempNameSuffix: "tmp",
		replicate:      replicate,
		cache: cache,
	}
}

func (fw *fileWriter) writeToTemp(contents []byte, filename string, replicateToFilename string) error {
	return nil
}

func (fw *fileWriter) finalize() error {

	// TODO: rename temp files to target filenames.
	// TODO: add to cache.
	// TODO: Replicate to replicate path/filenames.

	return nil
}
