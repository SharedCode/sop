package replication

import (
	"fmt"
	"os"

	"github.com/SharedCode/sop/fs"
)

type dioReplicationSim struct {
}

func newDirectIOReplicationSim() fs.UnitTestInjectableIO {
	return &dioReplicationSim{}
}

func (dio *dioReplicationSim) Open(filename string, flag int, permission os.FileMode) error {
	return fs.ReplicationRelatedError{
		Err: fmt.Errorf("simulated error on Open"),
	}
}
func (dio *dioReplicationSim) WriteAt(block []byte, offset int64) (int, error) {
	return 0, nil
}
func (dio *dioReplicationSim) ReadAt(block []byte, offset int64) (int, error) {
	return 0, nil
}
func (dio *dioReplicationSim) Close() error {
	return nil
}
