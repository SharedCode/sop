package redis

import (
	"crypto/tls"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Options holds configuration for connecting to a Redis server or cluster.
type Options struct {
	// Address is the host:port of the Redis server/cluster.
	Address string
	// Password is the password used to authenticate.
	Password string
	// DB is the database index to select.
	DB int
	// TLSConfig contains TLS configuration for secure connections.
	TLSConfig *tls.Config
}

// Connection wraps a redis.Client and the Options used to create it.
type Connection struct {
	Client  *redis.Client
	Options Options
}

// DefaultOptions returns an Options with localhost defaults (no password, DB 0).
func DefaultOptions() Options {
	return Options{
		Address:  "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}
}

var connection *Connection
var mux sync.Mutex

// IsConnectionInstantiated reports whether the package-level singleton connection exists.
func IsConnectionInstantiated() bool {
	return connection != nil
}

// OpenConnection initializes and returns the package-level singleton connection.
// Subsequent calls return the same connection.
func OpenConnection(options Options) (*Connection, error) {
	if connection != nil {
		return connection, nil
	}
	mux.Lock()
	defer mux.Unlock()

	if connection != nil {
		return connection, nil
	}

	connection = openConnection(options)
	return connection, nil
}

// CloseConnection closes the package-level singleton connection, if present.
func CloseConnection() error {
	if connection == nil {
		return nil
	}
	mux.Lock()
	defer mux.Unlock()
	if connection == nil {
		return nil
	}
	err := closeConnection(connection)
	connection = nil
	return err
}

// openConnection creates a new redis client connection from options.
func openConnection(options Options) *Connection {
	client := redis.NewClient(&redis.Options{
		TLSConfig: options.TLSConfig,
		Addr:      options.Address,
		Password:  options.Password,
		DB:        options.DB})

	c := Connection{
		Client:  client,
		Options: options,
	}
	return &c
}

// closeConnection closes the given connection, if not already closed.
func closeConnection(c *Connection) error {
	if c == nil || c.Client == nil {
		return nil
	}
	err := c.Client.Close()
	c.Client = nil
	return err
}
