package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/ncw/directio"
)

// For use in unit test only.
type UnitTestInjectableIO interface {
	// Open the file with a given filename.
	Open(filename string, flag int, permission os.FileMode) error
	WriteAt(block []byte, offset int64) (int, error)
	ReadAt(block []byte, offset int64) (int, error)
	Close() error
}

type directIO struct {
	file     *os.File
	filename string
}

const (
	blockSize = directio.BlockSize
)

// Allows unit test to inject a fake or a simulator.
var DirectIOSim UnitTestInjectableIO

// Instantiate a direct File IO object.
func newDirectIO() *directIO {
	return &directIO{}
}

// Open the file with a given filename.
func (dio *directIO) Open(filename string, flag int, permission os.FileMode) error {
	if DirectIOSim != nil {
		return DirectIOSim.Open(filename, flag, permission)
	}
	if dio.file != nil {
		return fmt.Errorf("there is an opened file for this directIO object, 'not allowed to open file again")
	}
	f, err := directio.OpenFile(filename, flag, permission)
	if err != nil {
		return err
	}
	dio.file = f
	dio.filename = filename
	return nil
}

func (dio *directIO) WriteAt(block []byte, offset int64) (int, error) {
	if DirectIOSim != nil {
		return DirectIOSim.WriteAt(block, offset)
	}
	if dio.file == nil {
		return 0, fmt.Errorf("can't write, there is no opened file")
	}
	return dio.file.WriteAt(block, offset)
}

func (dio *directIO) ReadAt(block []byte, offset int64) (int, error) {
	if DirectIOSim != nil {
		return DirectIOSim.ReadAt(block, offset)
	}
	if dio.file == nil {
		return 0, fmt.Errorf("can't read, there is no opened file")
	}
	return dio.file.ReadAt(block, offset)
}

func (dio *directIO) Close() error {
	if DirectIOSim != nil {
		return DirectIOSim.Close()
	}

	if dio.file == nil {
		return nil
	}

	err := dio.file.Close()
	dio.file = nil
	dio.filename = ""
	return err
}

func (dio *directIO) fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func (dio *directIO) getFileSize(filePath string) (int64, error) {
	s, err := os.Stat(filePath)
	return s.Size(), err
}

func (dio *directIO) isEOF(err error) bool {
	return io.EOF == err
}

// Create a buffer that is aligned to the file sector size, usable as buffer for reading file data, directly.
func (dio *directIO) createAlignedBlock() []byte {
	return dio.createAlignedBlockOfSize(directio.BlockSize)
}

// Create a buffer that is aligned to the file sector size, usable as buffer for reading file data, directly.
func (dio *directIO) createAlignedBlockOfSize(blockSize int) []byte {
	return directio.AlignedBlock(blockSize)
}
