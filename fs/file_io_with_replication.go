package fs

import (
	"github.com/SharedCode/sop"
)

type fileIO struct {
	filenames          []string
	manageStore        sop.ManageStore
	replicationTracker *replicationTracker
}

func newFileIOWithReplication(replicationTracker *replicationTracker, manageStore sop.ManageStore) *fileIO {
	return &fileIO{
		manageStore:        manageStore,
		replicationTracker: replicationTracker,
	}
}

func (fio *fileIO) write(targetFilename string, contents []byte) error {
	f := NewDefaultFileIO(nil)
	filename := fio.replicationTracker.formatActiveFolderFilename(targetFilename)
	fio.filenames = append(fio.filenames, targetFilename)
	return f.WriteFile(filename, contents, permission)
}

func (fio *fileIO) read(sourceFilename string) ([]byte, error) {
	f := NewDefaultFileIO(nil)
	filename := fio.replicationTracker.formatActiveFolderFilename(sourceFilename)
	return f.ReadFile(filename)
}

func (fio *fileIO) createStore(folderName string) error {
	f := NewDefaultFileIO(nil)
	filename := fio.replicationTracker.formatActiveFolderFilename(folderName)
	return f.MkdirAll(filename, permission)
}

func (fio *fileIO) replicate() error {

	// TODO: Replicate to replicate path/filenames.
	// TODO: decide whether failure on replication will be persisted, logged and thus, prevent future replication to occur.

	return nil
}
