package cache

import (
	"context"
	"encoding/json"
	"fmt"

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

func NewClient(options Options) Cache {
	client := redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB})

	c := Connection{
		Client:  client,
		Options: options,
	}
	return &c
}

// Ping tests connectivity for redis (PONG should be returned)
func (connection *Connection) Ping(ctx context.Context) error {
	pong, err := connection.Client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	fmt.Println(pong, err)
	// Output: PONG <nil>

	return nil
}

// Set executes the redis Set command
func (c *Connection) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	if expiration < 0 {
		expiration = c.Options.GetDefaultDuration()
	}
	return c.Client.Set(ctx, key, value, expiration).Err()
}

// Get executes the redis Get command
func (c *Connection) Get(ctx context.Context, key string) (string, error) {
	return c.Client.Get(ctx, key).Result()
}

// SetStruct executes the redis Set command
func (c *Connection) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	// serialize User object to JSON
	json, err := json.Marshal(value)
	if err != nil {
		return err
	}
	// SET object
	if expiration < 0 {
		expiration = c.Options.GetDefaultDuration()
	}
	return c.Client.Set(ctx, key, json, expiration).Err()
}

// GetStruct executes the redis Get command
func (c *Connection) GetStruct(ctx context.Context, key string, target interface{}) (interface{}, error) {
	if target == nil {
		panic("target can't be nil.")
	}
	s, err := c.Client.Get(ctx, key).Result()
	if err == nil {
		err = json.Unmarshal([]byte(s), target)
	}
	if err == redis.Nil {
		return nil, err
	}
	return target, err
}

// Delete executes the redis Del command
func (c *Connection) Delete(ctx context.Context, key string) error {
	var r = c.Client.Del(ctx, key)
	return r.Err()
}
