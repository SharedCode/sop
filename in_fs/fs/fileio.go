package fs


import (
	"os"
)

// Functions for File I/O defaults to "os" file I/O functions.
type FileIO interface {
	WriteFile(name string, data []byte, perm os.FileMode) error
	ReadFile(name string) ([]byte, error)
	Remove(name string) error

	Lock(name string) (bool, error)
	Unlock(name string) (error)
}

type defaultFileIO struct {
}

func (dio defaultFileIO)WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}
func (dio defaultFileIO)ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}
func (dio defaultFileIO)Remove(name string) error {
	return os.Remove(name)
}

func (dio defaultFileIO)Lock(name string) (bool, error) {
	return true, nil
}

func (dio defaultFileIO)Unlock(name string) error {
	return nil
}
