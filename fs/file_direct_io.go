package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/ncw/directio"
)

type fileDirectIO struct {
	file     *os.File
	filename string
	directIO DirectIO
}

// Allows unit test to inject a fake or a simulator.
var DirectIOSim DirectIO

// Instantiate a direct File IO object.
func newFileDirectIO() *fileDirectIO {
	directIO := DirectIOSim
	if directIO == nil {
		directIO = NewDirectIO()
	}
	return &fileDirectIO{
		directIO: directIO,
	}
}

// open the file with a given filename.
func (fio *fileDirectIO) open(filename string, flag int, permission os.FileMode) error {
	if fio.file != nil {
		return fmt.Errorf("there is an opened file for this directIO object, 'not allowed to open file again")
	}
	f, err := fio.directIO.Open(filename, flag, permission)
	if err != nil {
		return err
	}
	fio.file = f
	fio.filename = filename
	return nil
}

func (fio *fileDirectIO) writeAt(block []byte, offset int64) (int, error) {
	if fio.file == nil {
		return 0, fmt.Errorf("can't write, there is no opened file")
	}
	return fio.directIO.WriteAt(fio.file, block, offset)
}

func (fio *fileDirectIO) readAt(block []byte, offset int64) (int, error) {
	if fio.file == nil {
		return 0, fmt.Errorf("can't read, there is no opened file")
	}
	return fio.directIO.ReadAt(fio.file, block, offset)
}

func (fio *fileDirectIO) close() error {
	if fio.file == nil {
		return nil
	}

	err := fio.directIO.Close(fio.file)
	fio.file = nil
	fio.filename = ""
	return err
}

func (fio *fileDirectIO) fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func (fio *fileDirectIO) getFileSize(filePath string) (int64, error) {
	s, err := os.Stat(filePath)
	return s.Size(), err
}

func (fio *fileDirectIO) isEOF(err error) bool {
	return io.EOF == err
}

// Create a buffer that is aligned to the file sector size, usable as buffer for reading file data, directly.
func (fio *fileDirectIO) createAlignedBlock() []byte {
	return fio.createAlignedBlockOfSize(directio.BlockSize)
}

// Create a buffer that is aligned to the file sector size, usable as buffer for reading file data, directly.
func (fio *fileDirectIO) createAlignedBlockOfSize(blockSize int) []byte {
	return directio.AlignedBlock(blockSize)
}
