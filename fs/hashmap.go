package fs

import (
	"os"

	"github.com/SharedCode/sop"
)

type hashmap struct {
	hashModValue int
	replicationTracker  *replicationTracker
	// File handles of all known (traversed & opened) data segment file of the hash map.
	fileHandles map[string]*os.File
}

//TODO: perhaps methods will have built-in locking for thread safety.
// Use syscall for cross platform file region locking.

func newHashmap(hashModValue int, replicationTracker *replicationTracker) *hashmap {
	return &hashmap{
		hashModValue: hashModValue,
		replicationTracker: replicationTracker,
		fileHandles: make(map[string]*os.File),
	}
}

func (h *hashmap)get(keys ...sop.Tuple[string, []sop.UUID]) ([]sop.Handle, error) {
	// f, err := os.Open(h.filename)
	// if err != nil {
	// 	return TV{}, nil
	// }
	return nil, nil
}

func (h *hashmap)set(allOrNothing bool, items ...sop.Tuple[string, []sop.Handle]) error {
	return nil
}

func (h *hashmap)remove(keys ...sop.Tuple[string, []sop.UUID]) error {
	return nil
}

func (h *hashmap)close() error {
	var lastError error
	if h.fileHandles == nil {
		return nil
	}
	for _, f := range h.fileHandles {
		if err := f.Close(); err != nil {
			lastError = err
		}
	}
	h.fileHandles = nil
	return lastError
}
