package port

type Tx interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, val []byte) error
	Delete(key []byte) error
	Commit() error
	Rollback() error
}

type Capabilities struct {
	MaxValueSize  int
	DurableWrites bool
	SupportsBatch bool
}

type KVStore interface {
	Begin(readOnly bool) (Tx, error)
	Capabilities() Capabilities
	Stats() (map[string]any, error)
	Close() error
}
