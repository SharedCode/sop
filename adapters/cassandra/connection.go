// Package cassandra provides Cassandra-backed implementations of SOP repositories and logs,
// including connection/session management and per-API consistency customization.
package cassandra

import (
	"fmt"
	"sync"
	"time"

	log "log/slog"

	"github.com/gocql/gocql"
)

// Config contains configuration for connecting to a Cassandra cluster and SOP keyspace.
type Config struct {
	// ClusterHosts lists contact points for the Cassandra cluster.
	ClusterHosts []string
	// Keyspace is the keyspace used for SOP tables.
	Keyspace string
	// Consistency is the default consistency level for queries.
	Consistency gocql.Consistency
	// ConnectionTimeout is the session connection timeout.
	ConnectionTimeout time.Duration
	// Authenticator is used when the cluster requires authentication.
	Authenticator gocql.Authenticator
	// ReplicationClause defines the keyspace replication (e.g., SimpleStrategy).
	ReplicationClause string

	// ConsistencyBook allows overriding per-API consistency levels.
	ConsistencyBook ConsistencyBook
}

// ConsistencyBook enumerates per-API consistency levels used by this package.
type ConsistencyBook struct {
	RegistryAdd    gocql.Consistency
	RegistryUpdate gocql.Consistency
	RegistryGet    gocql.Consistency
	RegistryRemove gocql.Consistency
	StoreAdd       gocql.Consistency
	StoreUpdate    gocql.Consistency
	StoreGet       gocql.Consistency
	StoreRemove    gocql.Consistency

	// Blob store consistency levels are only used when the blob backend is Cassandra.
	BlobStoreAdd    gocql.Consistency
	BlobStoreGet    gocql.Consistency
	BlobStoreUpdate gocql.Consistency
	BlobStoreRemove gocql.Consistency
}

// Connection wraps a Cassandra session and its configuration.
type Connection struct {
	Session *gocql.Session
	Config
}

var session *gocql.Session
var config Config
var refCount int
var mux sync.Mutex

// IsConnectionInstantiated reports whether a global Connection has been created.
func IsConnectionInstantiated() bool {
	return session != nil
}

// OpenConnection returns the existing global Connection or opens a new one using the provided config.
func OpenConnection(cfg Config) (*Connection, error) {
	mux.Lock()
	defer mux.Unlock()

	if session == nil {
		log.Info("Opening Cassandra connection", "hosts", cfg.ClusterHosts, "keyspace", cfg.Keyspace)
		if cfg.Keyspace == "" {
			// default keyspace
			cfg.Keyspace = "btree"
		}
		if cfg.Consistency == gocql.Any {
			// Defaults to LocalQuorum consistency. You should set it to an appropriate level.
			cfg.Consistency = gocql.LocalQuorum
		}
		cluster := gocql.NewCluster(cfg.ClusterHosts...)
		cluster.Consistency = cfg.Consistency
		if cfg.ReplicationClause == "" {
			// Specify an appropriate replication feature.
			cfg.ReplicationClause = "{'class':'SimpleStrategy', 'replication_factor':1}"
		}
		if cfg.ConnectionTimeout > 0 {
			cluster.ConnectTimeout = cfg.ConnectionTimeout
		}
		if cfg.Authenticator != nil {
			cluster.Authenticator = cfg.Authenticator
			// Clear the authenticator just to be safer, we don't need to keep it hanging around.
			cfg.Authenticator = nil
		}
		s, err := cluster.CreateSession()
		if err != nil {
			return nil, fmt.Errorf("failed to create cassandra session: %w", err)
		}
		session = s
		config = cfg
	}

	if err := initKeyspace(session, cfg); err != nil {
		return nil, err
	}

	refCount++
	return &Connection{
		Session: session,
		Config:  cfg,
	}, nil
}

// GetConnection returns a connection for the given keyspace, reusing the global session if available.
func GetConnection(keyspace string) (*Connection, error) {
	mux.Lock()
	defer mux.Unlock()

	if session == nil {
		return nil, fmt.Errorf("cassandra connection is closed; call OpenConnection(config) to open it")
	}

	cfg := config
	cfg.Keyspace = keyspace
	if cfg.Keyspace == "" {
		cfg.Keyspace = "btree"
	}

	if err := initKeyspace(session, cfg); err != nil {
		return nil, err
	}

	refCount++
	return &Connection{
		Session: session,
		Config:  cfg,
	}, nil
}

// GetGlobalConnection returns the global connection using the global configuration.
func GetGlobalConnection() (*Connection, error) {
	mux.Lock()
	defer mux.Unlock()

	if session == nil {
		return nil, fmt.Errorf("cassandra connection is closed; call OpenConnection(config) to open it")
	}

	return &Connection{
		Session: session,
		Config:  config,
	}, nil
}

// NewConnection opens a new connection using the provided config.
// Deprecated: Use OpenConnection instead.
func NewConnection(config Config) (*Connection, error) {
	return OpenConnection(config)
}

func initKeyspace(s *gocql.Session, config Config) error {
	if err := s.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s;", config.Keyspace, config.ReplicationClause)).Exec(); err != nil {
		return fmt.Errorf("failed to create keyspace %s: %w", config.Keyspace, err)
	}
	// Auto create the "store" table if not yet.
	if err := s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.store (name text PRIMARY KEY, root_id UUID, slot_count int, count bigint, unique boolean, des text, reg_tbl text, blob_tbl text, ts bigint, vdins boolean, vdap boolean, vdgc boolean, llb boolean, rcd bigint, rc_ttl boolean, ncd bigint, nc_ttl boolean, vdcd bigint, vdc_ttl boolean, scd bigint, sc_ttl boolean);", config.Keyspace)).Exec(); err != nil {
		return fmt.Errorf("failed to create store table: %w", err)
	}
	if err := s.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.t_log (id UUID, c_f int, c_f_p blob, PRIMARY KEY(id, c_f));", config.Keyspace)).Exec(); err != nil {
		return fmt.Errorf("failed to create t_log table: %w", err)
	}
	return nil
}

// CloseConnection closes and clears the global connection, if it exists.
func CloseConnection() {
	mux.Lock()
	defer mux.Unlock()
	if session != nil {
		log.Info("Closing Cassandra connection")
		session.Close()
		session = nil
		refCount = 0
	}
}

// Close closes the connection.
func (c *Connection) Close() {
	mux.Lock()
	defer mux.Unlock()
	refCount--
	if refCount <= 0 && session != nil {
		log.Info("Closing Cassandra connection")
		session.Close()
		session = nil
		refCount = 0
	}
}
