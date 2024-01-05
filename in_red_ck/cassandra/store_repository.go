package cassandra

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
	"github.com/gocql/gocql"
)

// TODO: when need arise, move these interfaces to a common package, but keep them for now
// in package where they are implemented, 'just because we wanted to keep changes minimal,
// and driven by needs.

// StoreRepository interface specifies the store repository.
type StoreRepository interface {
	// Fetch store info with name.
	Get(context.Context, ...string) ([]btree.StoreInfo, error)
	// Add store info.
	Add(context.Context, ...btree.StoreInfo) error
	// Update store info.
	// Update should also merge the Count of items between the incoming store info
	// and the target store info on the backend, as they may differ. It should use
	// StoreInfo.CountDelta to reconcile the two.
	Update(context.Context, ...btree.StoreInfo) error
	// Remove store info with name.
	Remove(context.Context, ...string) error
}

type storeRepository struct{}

// NewStoreRepository manages the StoreInfo in Cassandra table.
func NewStoreRepository() StoreRepository {
	return &storeRepository{}
}

// Add a new store record, create a new Virtual ID registry and node blob tables.
func (sr *storeRepository) Add(ctx context.Context, stores ...btree.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	insertStatement := fmt.Sprintf("INSERT INTO %s.store (name, root_id, slot_count, count, unique, des, reg_tbl, blob_tbl, ts, vdins, llb) VALUES(?,?,?,?,?,?,?,?,?,?,?);", connection.Config.Keyspace)
	for _, s := range stores {
		// Add a new store record.
		if err := connection.Session.Query(insertStatement, s.Name, gocql.UUID(s.RootNodeId), s.SlotLength, s.Count, s.IsUnique, s.Description,
			s.RegistryTable, s.BlobTable, s.Timestamp, s.IsValueDataInNodeSegment, s.LeafLoadBalancing).Exec(); err != nil {
			return err
		}
		// Create a new Blob table.
		createNewBlobTable := fmt.Sprintf("CREATE TABLE %s.%s(id UUID PRIMARY KEY, node blob);", connection.Config.Keyspace, s.BlobTable)
		if err := connection.Session.Query(createNewBlobTable).Exec(); err != nil {
			return err
		}
		// Create a new Virtual ID registry table.
		createNewRegistry := fmt.Sprintf("CREATE TABLE %s.%s(lid UUID PRIMARY KEY, is_idb boolean, p_ida UUID, p_idb UUID, ts bigint, wip_ts bigint, is_del boolean);",
			connection.Config.Keyspace, s.RegistryTable)
		if err := connection.Session.Query(createNewRegistry).Exec(); err != nil {
			return err
		}
	}
	return nil
}

// Update enforces so only the Store's Count & timestamp can get updated.
func (sr *storeRepository) Update(ctx context.Context, stores ...btree.StoreInfo) error {
	if connection == nil {
		return fmt.Errorf("Cassandra connection is closed, 'call GetConnection(config) to open it.")
	}
	updateStatement := fmt.Sprintf("UPDATE %s.store SET count = count + ?, ts = ? WHERE name = ?;", connection.Config.Keyspace)
	batch := connection.Session.NewBatch(gocql.UnloggedBatch)
	for _, s := range stores {
		// Update store record.
		batch.Query(updateStatement,  s.CountDelta, s.Timestamp, s.Name)
	}
	return connection.Session.ExecuteBatch(batch)
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
