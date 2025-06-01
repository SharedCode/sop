package fs

import "os"

type dioReplicationSim struct {
}

func newDirectIOReplicationSim() unitTestInjectableIO {
	return &dioReplicationSim{}
}

func (dio *dioReplicationSim) open(filename string, flag int, permission os.FileMode) error {
	return nil
}
func (dio *dioReplicationSim) writeAt(block []byte, offset int64) (int, error) {
	return 0, nil
}
func (dio *dioReplicationSim) readAt(block []byte, offset int64) (int, error) {
	return 0, nil
}
func (dio *dioReplicationSim) close() error {
	return nil
}
