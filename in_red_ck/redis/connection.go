package redis

import (
	"sync"

	"github.com/redis/go-redis/v9"
	"time"
)

type Options struct {
	Address                  string
	Password                 string
	DB                       int
	DefaultDurationInSeconds int
}

func (opt *Options) GetDefaultDuration() time.Duration {
	return time.Duration(opt.DefaultDurationInSeconds) * time.Second
}

type Connection struct {
	Client  *redis.Client
	Options Options
}

func DefaultOptions() Options {
	return Options{
		Address:                  "localhost:6379",
		Password:                 "", // no password set
		DB:                       0,  // use default DB
		DefaultDurationInSeconds: 24 * 60 * 60,
	}
}

var connection *Connection
var mux sync.Mutex

// Creates a singleton connection and returns it for every call.
func GetConnection(options Options) (*Connection, error) {
	if connection != nil {
		return connection, nil
	}
	mux.Lock()
	defer mux.Unlock()

	if connection != nil {
		return connection, nil
	}

	client := redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB})

	c := Connection{
		Client:  client,
		Options: options,
	}
	connection = &c
	return connection, nil
}

// Close the singleton connection if open.
func CloseConnection() {
	if connection != nil {
		mux.Lock()
		defer mux.Unlock()
		if connection == nil {
			return
		}
		connection.Client.Close()
		connection = nil
	}
}
