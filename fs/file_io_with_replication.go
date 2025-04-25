package fs

import (
	"fmt"
	"os"
	"strings"

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
	filename := fio.replicationTracker.storesBaseFolders[0]
	if !fio.replicationTracker.isFirstFolderActive {
		filename = fio.replicationTracker.storesBaseFolders[1]
	}
	if strings.HasSuffix(filename, string(os.PathSeparator)) {
		filename = fmt.Sprintf("%s%s", filename, targetFilename)
	} else {
		filename = fmt.Sprintf("%s%c%s", filename, os.PathSeparator, targetFilename)
	}
	fio.filenames = append(fio.filenames, targetFilename)
	return f.WriteFile(filename, contents, permission)
}

func (fio *fileIO) read(sourceFilename string) ([]byte, error) {
	f := NewDefaultFileIO(nil)
	filename := fio.replicationTracker.storesBaseFolders[0]
	if !fio.replicationTracker.isFirstFolderActive {
		filename = fio.replicationTracker.storesBaseFolders[1]
	}
	if strings.HasSuffix(filename, string(os.PathSeparator)) {
		filename = fmt.Sprintf("%s%s", filename, sourceFilename)
	} else {
		filename = fmt.Sprintf("%s%c%s", filename, os.PathSeparator, sourceFilename)
	}

	return f.ReadFile(filename)
}

func (fio *fileIO) createStore(folderName string) error {
	f := NewDefaultFileIO(nil)
	filename := fio.replicationTracker.storesBaseFolders[0]
	if !fio.replicationTracker.isFirstFolderActive {
		filename = fio.replicationTracker.storesBaseFolders[1]
	}
	if strings.HasSuffix(filename, string(os.PathSeparator)) {
		filename = fmt.Sprintf("%s%s", filename, folderName)
	} else {
		filename = fmt.Sprintf("%s%c%s", filename, os.PathSeparator, folderName)
	}
	return f.MkdirAll(filename, permission)
}

func (fio *fileIO) replicate() error {

	// TODO: Replicate to replicate path/filenames.
	// TODO: decide whether failure on replication will be persisted, logged and thus, prevent future replication to occur.

	return nil
}
