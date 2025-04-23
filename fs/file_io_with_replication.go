package fs

import (
	"context"

	"github.com/SharedCode/sop"
)

type fileIO struct {
	filenames     [][]string
	manageStore   sop.ManageStore
	replicationTracker  *replicationTracker
}

func newFileIOWithReplication(replicationTracker *replicationTracker, manageStore sop.ManageStore) *fileIO {
	return &fileIO{
		manageStore: manageStore,
		replicationTracker: replicationTracker,
	}
}

func (fio *fileIO) write(contents []byte, targetFolders []string, targetFilename string) error {
	// TODO
	return nil
}

func (fio *fileIO) read(sourceFolders []string, filename string) ([]byte, error) {
	// TODO
	// fn := fmt.Sprintf("%s%cstorelist.txt", sr.storesBaseFolders[0], os.PathSeparator)
	// if sr.replicate && !sr.isFirstFolderActive {
	// 	fn = fmt.Sprintf("%s%cstorelist.txt", sr.storesBaseFolders[1], os.PathSeparator)
	// }
	// ba, err := sr.fileIO.ReadFile(fn)
	// if err != nil {
	// 	return nil, err
	// }
	return nil, nil
}

func (fio *fileIO) createStore(ctx context.Context, targetFolders []string, folderName string) error {
	return nil
}

func (fio *fileIO) replicate() error {

	// TODO: Replicate to replicate path/filenames.
	// TODO: decide whether failure on replication will be persisted, logged and thus, prevent future replication to occur.

	return nil
}
