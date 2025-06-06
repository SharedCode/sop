package fs

import (
	"os"
	"path/filepath"
)

// Functions for File I/O defaults to "os" file I/O functions.
type FileIO interface {
	WriteFile(name string, data []byte, perm os.FileMode) error
	ReadFile(name string) ([]byte, error)
	Remove(name string) error
	Exists(path string) bool

	// Directory API.
	RemoveAll(path string) error
	MkdirAll(path string, perm os.FileMode) error
}

type DefaultFileIO struct {
}

func NewDefaultFileIO() FileIO {
	return &DefaultFileIO{}
}

func (dio DefaultFileIO) WriteFile(name string, data []byte, perm os.FileMode) error {
	if err := os.WriteFile(name, data, perm); err != nil {
		dirPath := filepath.Dir(name)
		if derr := dio.MkdirAll(dirPath, permission); derr == nil {
			return os.WriteFile(name, data, perm)
		}
		return err
	}
	return nil
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
