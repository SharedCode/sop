package fs

import (
	"os"

	"github.com/SharedCode/sop"
)

// Functions for File I/O defaults to "os" file I/O functions.
type FileIO interface {

	// ToFilePath is part of FileIO so we can allow implementations to drive
	// generation of full path filename.
	ToFilePath(basePath string, id sop.UUID) string

	WriteFile(name string, data []byte, perm os.FileMode) error
	ReadFile(name string) ([]byte, error)
	Remove(name string) error
	Exists(path string) bool

	// Directory API.
	RemoveAll(path string) error
	MkdirAll(path string, perm os.FileMode) error
}

type DefaultFileIO struct {
	toFilePath ToFilePathFunc
}

func NewDefaultFileIO(toFilePath ToFilePathFunc) FileIO {
	return &DefaultFileIO{
		toFilePath: toFilePath,
	}
}

func (dio DefaultFileIO) ToFilePath(basePath string, id sop.UUID) string {
	return dio.toFilePath(basePath, id)
}

func (dio DefaultFileIO) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}
func (dio DefaultFileIO) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}
func (dio DefaultFileIO) Remove(name string) error {
	return os.Remove(name)
}

func (dio DefaultFileIO) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}
func (dio DefaultFileIO) RemoveAll(path string) error {
	return os.RemoveAll(path)
}
func (dio DefaultFileIO) Exists(path string) bool {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return true
	}
	return false
}
