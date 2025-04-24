package fs

import (
	"os"

	"github.com/SharedCode/sop"
)

type hashmap[TK sop.UUID, TV sop.Handle] struct {
	filename string
	hashModValue int
	replicationTracker  *replicationTracker
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles map[string]*os.File
}

//TODO: perhaps methods will have built-in locking for thread safety.
// Use syscall for cross platform file region locking.

func newHashmap(filename string, hashModValue int, replicationTracker *replicationTracker) *hashmap[sop.UUID, sop.Handle] {
	return &hashmap[sop.UUID, sop.Handle]{
		filename: filename,
		hashModValue: hashModValue,
		replicationTracker: replicationTracker,
		fileHandles: make(map[string]*os.File),
	}
}

func (h *hashmap[TK, TV])get(k TK) (TV, error) {
	// f, err := os.Open(h.filename)
	// if err != nil {
	// 	return TV{}, nil
	// }
	return TV{}, nil
}

func (h *hashmap[TK, TV])setAllOrNothing(items ...TV) error {
	return nil
}

func (h *hashmap[TK, TV])set(k TK, v TV) error {
	return nil
}

func (h *hashmap[TK, TV])remove(k TK) error {
	return nil
}

func (h *hashmap[TK, TV])close() error {
	var lastError error
	for _, f := range h.fileHandles {
		if err := f.Close(); err != nil {
			lastError = err
		}
	}
	return lastError
}
