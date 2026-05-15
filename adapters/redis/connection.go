// Package redis provides a Cache implementation and simple distributed locking built on Redis.
package redis

import (
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	log "log/slog"

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

	// DialTimeout specifies the timeout for connecting to Redis.
	DialTimeout time.Duration
	// ReadTimeout specifies the timeout for reading from Redis.
	ReadTimeout time.Duration
	// WriteTimeout specifies the timeout for writing to Redis.
	WriteTimeout time.Duration
	// MaxRetries is the maximum number of retries before giving up on Redis connection.
	MaxRetries int
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

	log.Info("Opening Redis connection", "address", options.Address, "db", options.DB)
	connection = openConnection(options)
	return connection, nil
}

// OpenConnectionWithURL initializes and returns the package-level singleton connection using a Redis URI.
func OpenConnectionWithURL(url string) (*Connection, error) {
	if connection != nil {
		return connection, nil
	}
	mux.Lock()
	defer mux.Unlock()

	if connection != nil {
		return connection, nil
	}

	log.Info("Opening Redis connection with URL")
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}

	connection = openConnectionFromRedisOptions(opts)
	log.Info("Redis connection established", "connection", connection)
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
	log.Info("Closing Redis connection")
	err := closeConnection(connection)
	connection = nil
	return err
}

var hasRestarted int64

// openConnection creates a new redis client connection from options.
func openConnection(options Options) *Connection {
	dialTimeout := options.DialTimeout
	if dialTimeout == 0 {
		dialTimeout = 2 * time.Second
	}
	readTimeout := options.ReadTimeout
	if readTimeout == 0 {
		readTimeout = 2 * time.Second
	}
	writeTimeout := options.WriteTimeout
	if writeTimeout == 0 {
		writeTimeout = 2 * time.Second
	}
	maxRetries := options.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	return openConnectionFromRedisOptions(&redis.Options{
		TLSConfig:    options.TLSConfig,
		Addr:         options.Address,
		Password:     options.Password,
		DB:           options.DB,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		MaxRetries:   maxRetries,
	})
}

func openConnectionFromRedisOptions(opts *redis.Options) *Connection {
	client := redis.NewClient(opts)

	c := Connection{
		Client: client,
		Options: Options{
			Address:      opts.Addr,
			Password:     opts.Password,
			DB:           opts.DB,
			TLSConfig:    opts.TLSConfig,
			DialTimeout:  opts.DialTimeout,
			ReadTimeout:  opts.ReadTimeout,
			WriteTimeout: opts.WriteTimeout,
			MaxRetries:   opts.MaxRetries,
		},
	}
	return &c
}

// closeConnection closes the given connection, if not already closed.
func closeConnection(c *Connection) error {
	if c == nil || c.Client == nil {
		return nil
	}
	log.Debug("Closing underlying Redis client")
	err := c.Client.Close()
	c.Client = nil
	return err
}
