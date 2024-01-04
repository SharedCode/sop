package cassandra

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
	// "github.com/gocql/gocql"
)

// TODO: when need arise, move these interfaces to a common package, but keep them for now
// in package where they are implemented, 'just because we wanted to keep changes minimal,
// and driven by needs.

// StoreRepository interface specifies the store repository.
type StoreRepository interface {
	// Fetch store info with name.
	Get(context.Context, ...string) ([]btree.StoreInfo, error)
	// Add store info. Add all or nothing.
	Add(context.Context, ...btree.StoreInfo) error
	// Update store info. Update all or nothing.
	// Update should also merge the Count of items between the incoming store info
	// and the target store info on the backend, as they may differ. It should use
	// StoreInfo.CountDelta to reconcile the two.
	Update(context.Context, ...btree.StoreInfo) error
	// Remove store info with name. Remove all or nothing.
	Remove(context.Context, ...string) error
}

type storeRepository struct {}

// NewStoreRepository manages the StoreInfo in Cassandra table.
func NewStoreRepository() StoreRepository {
	return &storeRepository{}
}

func (sr *storeRepository) Add(ctx context.Context, stores ...btree.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	// batch := connection.Session.NewBatch(gocql.CounterBatch)
	// batch.Query("")
	// for store := range stores {
	// 	// Create a new Blob table.
	// 	batch.Query("CREATE TABLE IF NOT EXISTS btree.%s (name text PRIMARY KEY, );").Exec()
	// 	// Create a new Virtual ID registry table.
	// }

	return nil
}

func (sr *storeRepository) Update(ctx context.Context, stores ...btree.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	return nil
}

func (sr *storeRepository) Get(ctx context.Context, names ...string) ([]btree.StoreInfo, error) {
	if connection == nil {
		return nil, fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	stores := make([]btree.StoreInfo, len(names))
	return stores, nil
}

func (sr *storeRepository) Remove(ctx context.Context, names ...string) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	return nil
}
