package fs

import (
	"context"

	"github.com/SharedCode/sop"
)

type fileWriter struct {
	filenames            [][]string
	tempFilenames        []string
	replicate            bool
	cache sop.Cache
	manageStore sop.ManageStore
}

const tempNameSuffix = "tmp"

func newFileWriterWithReplication(replicate bool, cache sop.Cache, manageStore sop.ManageStore) *fileWriter {
	return &fileWriter{
		replicate:      replicate,
		cache: cache,
		manageStore: manageStore,
	}
}

func (fw *fileWriter) writeToTemp(contents []byte, targetFolders []string, targetFilename string) error {
	return nil
}

func (fw *fileWriter) createStore(ctx context.Context, targetFolders []string, folderName string) error {
	return nil
}

func (fw *fileWriter) finalize() error {

	// TODO: rename temp files to target filenames.
	// TODO: add to cache.
	// TODO: Replicate to replicate path/filenames.

	return nil
}
