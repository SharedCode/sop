package fs

import(
	"os"

	"github.com/ncw/directio"
)

// For use in unit test only.
type DirectIO interface {
	// Open the file with a given filename.
	Open(filename string, flag int, permission os.FileMode) (*os.File, error)
	WriteAt(file *os.File, block []byte, offset int64) (int, error)
	ReadAt(file *os.File, block []byte, offset int64) (int, error)
	Close(file *os.File) error
}

const (
	blockSize = directio.BlockSize
)

type directIO struct {}

func NewDirectIO() DirectIO {
	return &directIO{}
}

func (dio directIO)	Open(filename string, flag int, permission os.FileMode) (*os.File, error) {
	return directio.OpenFile(filename, flag, permission)
}
func (dio directIO)	WriteAt(file *os.File, block []byte, offset int64) (int, error) {
	return file.WriteAt(block, offset)
}
func (dio directIO)	ReadAt(file *os.File, block []byte, offset int64) (int, error) {
	return file.ReadAt(block, offset)
}

func (dio directIO)	Close(file *os.File) error {
	return file.Close()
}
