package fs

import (
	"github.com/SharedCode/sop"
)

type fileIO struct {
	manageStore        sop.ManageStore
	replicationTracker *replicationTracker
	fio                FileIO
	fio                FileIO
}

func newFileIOWithReplication(replicationTracker *replicationTracker, manageStore sop.ManageStore) *fileIO {
	return &fileIO{
		manageStore:        manageStore,
		replicationTracker: replicationTracker,
		fio:                NewDefaultFileIO(nil),
	}
}

// TODO: Do we want to simplify these File IOs? New findings show we need just to write to the target
// replication paths during successful commit's cleanup, before the transaction logs are destroyed.

func (fio *fileIO) exists(targetFilename string) bool {
	filename := fio.replicationTracker.formatActiveFolderFilename(targetFilename)
	return fio.fio.Exists(filename)
		fio:                NewDefaultFileIO(nil),
	}
}

// TODO: Do we want to simplify these File IOs? New findings show we need just to write to the target
// replication paths during successful commit's cleanup, before the transaction logs are destroyed.

func (fio *fileIO) exists(targetFilename string) bool {
	filename := fio.replicationTracker.formatActiveFolderFilename(targetFilename)
	return fio.fio.Exists(filename)
}

func (fio *fileIO) write(targetFilename string, contents []byte) error {
	filename := fio.replicationTracker.formatActiveFolderFilename(targetFilename)
	return fio.fio.WriteFile(filename, contents, permission)
}

func (fio *fileIO) read(sourceFilename string) ([]byte, error) {
	filename := fio.replicationTracker.formatActiveFolderFilename(sourceFilename)
	return fio.fio.ReadFile(filename)
	return fio.fio.ReadFile(filename)
}

func (fio *fileIO) createStore(folderName string) error {
	filename := fio.replicationTracker.formatActiveFolderFilename(folderName)
	return fio.fio.MkdirAll(filename, permission)
}

func (fio *fileIO) removeStore(folderName string) error {
	filename := fio.replicationTracker.formatActiveFolderFilename(folderName)
	return fio.fio.RemoveAll(filename)
}

func (fio *fileIO) replicate() error {

	// TODO: Replicate to replicate path/filenames.
	// TODO: decide whether failure on replication will be persisted, logged and thus, prevent future replication to occur.

	return nil
}
