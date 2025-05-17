package redis

import (
	"crypto/tls"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Redis configurable options.
type Options struct {
	// Redis server(cluster) address.
	Address string
	// Password required when connecting to the Redis server.
	Password string
	// DB to connect to.
	DB int
	// TLS config.
	TLSConfig *tls.Config
}

// Connection contains Redis client connection object and the Options used to connect.
type Connection struct {
	Client  *redis.Client
	Options Options
}

// DefaultOptions.
func DefaultOptions() Options {
	return Options{
		Address:  "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}
}

var connection *Connection
var mux sync.Mutex

// Returns true if connection instance is valid.
func IsConnectionInstantiated() bool {
	return connection != nil
}

// Creates a singleton connection and returns it for every call.
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

// Close the singleton connection if open.
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

// Creates a singleton connection and returns it for every call.
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

// Close the singleton connection if open.
func closeConnection(c *Connection) error {
	if c == nil || c.Client == nil {
		return nil
	}
	err := c.Client.Close()
	c.Client = nil
	return err
}
