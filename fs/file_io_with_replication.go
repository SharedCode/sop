package fs

import (
	"github.com/SharedCode/sop"
	log "log/slog"
)

type fileIO struct {
	manageStore        sop.ManageStore
	replicationTracker *replicationTracker
	fio                FileIO
	// 1 = write, 2 = createStore, 3 = removeStore
	actionsDone []sop.Tuple[int, any]
}

func newFileIOWithReplication(replicationTracker *replicationTracker, manageStore sop.ManageStore) *fileIO {
	return &fileIO{
		manageStore:        manageStore,
		replicationTracker: replicationTracker,
		fio:                NewDefaultFileIO(nil),
	}
}

func (fio *fileIO) exists(targetFilename string) bool {
	filename := fio.replicationTracker.formatActiveFolderFilename(targetFilename)
	return fio.fio.Exists(filename)
}

func (fio *fileIO) write(targetFilename string, contents []byte) error {
	filename := fio.replicationTracker.formatActiveFolderFilename(targetFilename)	
	err := fio.fio.WriteFile(filename, contents, permission)
	fio.actionsDone = append(fio.actionsDone, sop.Tuple[int, any]{
		First: 1,
		Second: sop.Tuple[string, []byte]{
			First: targetFilename,
			Second: contents,
	}})
	return err
}

func (fio *fileIO) read(sourceFilename string) ([]byte, error) {
	filename := fio.replicationTracker.formatActiveFolderFilename(sourceFilename)
	return fio.fio.ReadFile(filename)
}

func (fio *fileIO) createStore(folderName string) error {
	filename := fio.replicationTracker.formatActiveFolderFilename(folderName)
	err := fio.fio.MkdirAll(filename, permission)
	fio.actionsDone = append(fio.actionsDone, sop.Tuple[int, any]{
		First: 2,
		Second: folderName,
	})
	return err
}

func (fio *fileIO) removeStore(folderName string) error {
	filename := fio.replicationTracker.formatActiveFolderFilename(folderName)
	err := fio.fio.RemoveAll(filename)
	fio.actionsDone = append(fio.actionsDone, sop.Tuple[int, any]{
		First: 3,
		Second: folderName,
	})
	return err
}

func (fio *fileIO) replicate() error {
	if !fio.replicationTracker.replicate {
		return nil
	}

	for i := range fio.actionsDone {
		switch fio.actionsDone[i].First {
		case 1:
			// write file.
			payload := fio.actionsDone[i].Second.(sop.Tuple[string,[]byte])
			targetFilename := fio.replicationTracker.formatPassiveFolderFilename(payload.First)
			return fio.fio.WriteFile(targetFilename, payload.Second, permission)

		case 2:
			// create store
			payload := fio.actionsDone[i].Second.(string)
			targetFolder := fio.replicationTracker.formatPassiveFolderFilename(payload)
			return fio.fio.MkdirAll(targetFolder, permission)

		case 3:
			// remove store
			payload := fio.actionsDone[i].Second.(string)
			targetFolder := fio.replicationTracker.formatPassiveFolderFilename(payload)
			return fio.fio.RemoveAll(targetFolder)

		default:
			log.Error("unsupported action type 3")
		}
	}

	return nil
}
