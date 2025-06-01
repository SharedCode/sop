package fs

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/SharedCode/sop"
)

type fileIOReplicationSimulator struct {
	lookup               map[string][]byte
	locker               sync.Mutex
	errorOnSuffixNumber  int
	errorOnSuffixNumber2 int
	reset                bool
}

func newFileIOReplicationSim() *fileIOReplicationSimulator {
	return &fileIOReplicationSimulator{
		lookup:               make(map[string][]byte),
		locker:               sync.Mutex{},
		errorOnSuffixNumber:  -1,
		errorOnSuffixNumber2: -1,
	}
}

// ToFilePath is part of FileIO so we can allow implementations to drive
// generation of full path filename.
func (sim *fileIOReplicationSimulator) ToFilePath(basePath string, id sop.UUID) string {
	return ""
}

func (sim *fileIOReplicationSimulator) WriteFile(name string, data []byte, perm os.FileMode) error {
	if sim.errorOnSuffixNumber >= 0 && strings.HasSuffix(name, fmt.Sprintf("_%d", sim.errorOnSuffixNumber)) {
		return fmt.Errorf("induced error on file suffix %d", sim.errorOnSuffixNumber)
	}
	sim.locker.Lock()
	sim.lookup[name] = data
	sim.locker.Unlock()
	return nil
}
func (sim *fileIOReplicationSimulator) ReadFile(name string) ([]byte, error) {
	if sim.errorOnSuffixNumber >= 0 && strings.HasSuffix(name, fmt.Sprintf("_%d", sim.errorOnSuffixNumber)) {
		return nil, fmt.Errorf("induced error on file suffix %d", sim.errorOnSuffixNumber)
	}
	if sim.errorOnSuffixNumber2 >= 0 && strings.HasSuffix(name, fmt.Sprintf("_%d", sim.errorOnSuffixNumber2)) {
		if sim.reset {
			sim.errorOnSuffixNumber = -1
			sim.errorOnSuffixNumber2 = -1
		}
		return nil, fmt.Errorf("induced error on file suffix %d", sim.errorOnSuffixNumber2)
	}
	sim.locker.Lock()
	defer sim.locker.Unlock()

	if _, ok := sim.lookup[name]; !ok {
		return nil, fmt.Errorf("file %s not found", name)
	}
	ba := sim.lookup[name]
	return ba, nil
}
func (sim *fileIOReplicationSimulator) Remove(name string) error {
	if sim.errorOnSuffixNumber >= 0 && strings.HasSuffix(name, fmt.Sprintf("_%d", sim.errorOnSuffixNumber)) {
		return fmt.Errorf("induced error on file suffix %d", sim.errorOnSuffixNumber)
	}
	sim.locker.Lock()
	delete(sim.lookup, name)
	sim.locker.Unlock()
	return nil
}
func (sim *fileIOReplicationSimulator) Exists(path string) bool {
	return true
}

// Directory API.
func (sim *fileIOReplicationSimulator) RemoveAll(path string) error {
	return nil
}
func (sim *fileIOReplicationSimulator) MkdirAll(path string, perm os.FileMode) error {
	return nil
}
