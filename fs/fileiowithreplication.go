package fs

import (
	"context"
	log "log/slog"

	"github.com/sharedcode/sop"
)

type fileIO struct {
	manageStore        sop.ManageStore
	replicationTracker *replicationTracker
	fio                FileIO
	trackActions       bool
	// 1 = write, 2 = createStore, 3 = removeStore
	actionsDone []sop.Tuple[int, any]
}

// Allows unit test to inject a simulated or fake FileIO for its test related peek/poke.
var FileIOSim FileIO

func newFileIOWithReplication(replicationTracker *replicationTracker, manageStore sop.ManageStore, trackActions bool) *fileIO {
	fio := NewFileIO()

	// Allow unit test to inject unit test "fake" for File IO.
	if FileIOSim != nil {
		fio = FileIOSim
	}

	return &fileIO{
		manageStore:        manageStore,
		replicationTracker: replicationTracker,
		fio:                fio,
		trackActions:       trackActions,
	}
}

func (fio *fileIO) exists(ctx context.Context, targetFilename string) bool {
	filename := fio.replicationTracker.formatActiveFolderEntity(targetFilename)
	return fio.fio.Exists(ctx, filename)
}

func (fio *fileIO) write(ctx context.Context, targetFilename string, contents []byte) error {
	filename := fio.replicationTracker.formatActiveFolderEntity(targetFilename)
	err := fio.fio.WriteFile(ctx, filename, contents, permission)
	if !fio.trackActions {
		return err
	}
	if err == nil {
		fio.actionsDone = append(fio.actionsDone, sop.Tuple[int, any]{
			First: 1,
			Second: sop.Tuple[string, []byte]{
				First:  targetFilename,
				Second: contents,
			}})
	}
	return err
}

func (fio *fileIO) read(ctx context.Context, sourceFilename string) ([]byte, error) {
	filename := fio.replicationTracker.formatActiveFolderEntity(sourceFilename)
	return fio.fio.ReadFile(ctx, filename)
}

func (fio *fileIO) createStore(ctx context.Context, folderName string) error {
	folderPath := fio.replicationTracker.formatActiveFolderEntity(folderName)
	err := fio.fio.MkdirAll(ctx, folderPath, permission)
	if !fio.trackActions {
		return err
	}
	if err == nil {
		fio.actionsDone = append(fio.actionsDone, sop.Tuple[int, any]{
			First:  2,
			Second: folderName,
		})
	}
	return err
}

func (fio *fileIO) removeStore(ctx context.Context, folderName string) error {
	filename := fio.replicationTracker.formatActiveFolderEntity(folderName)
	err := fio.fio.RemoveAll(ctx, filename)
	if !fio.trackActions {
		return err
	}
	if err == nil {
		fio.actionsDone = append(fio.actionsDone, sop.Tuple[int, any]{
			First:  3,
			Second: folderName,
		})
	}
	return err
}

func (fio *fileIO) replicate(ctx context.Context) error {
	if !fio.replicationTracker.replicate {
		return nil
	}

	for i := range fio.actionsDone {
		switch fio.actionsDone[i].First {
		case 1:
			// write file.
			payload := fio.actionsDone[i].Second.(sop.Tuple[string, []byte])
			targetFilename := fio.replicationTracker.formatPassiveFolderEntity(payload.First)
			if err := fio.fio.WriteFile(ctx, targetFilename, payload.Second, permission); err != nil {
				return err
			}
		case 2:
			// create store
			payload := fio.actionsDone[i].Second.(string)
			targetFolder := fio.replicationTracker.formatPassiveFolderEntity(payload)
			if err := fio.fio.MkdirAll(ctx, targetFolder, permission); err != nil {
				return err
			}
		case 3:
			// remove store
			payload := fio.actionsDone[i].Second.(string)
			targetFolder := fio.replicationTracker.formatPassiveFolderEntity(payload)
			if err := fio.fio.RemoveAll(ctx, targetFolder); err != nil {
				return err
			}

		default:
			log.Error("unsupported action type 3")
		}
	}

	fio.actionsDone = nil
	return nil
}
