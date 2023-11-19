package cache

import (
	"context"
	"time"
)

// TODO: implement this when need for in-memory cache arise, e.g. - faster proto-typing.

type store struct {
	items map[string]interface{}
}

func NewInMemory() Cache {
	return &store{}
}

// Set executes the redis Set command
func (s *store) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	return nil
}

// Get executes the redis Get command
func (s *store) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}

// SetStruct executes the redis Set command
func (s *store) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	// serialize User object to JSON
	// SET object
	// TODO
	return nil
}

// GetStruct executes the redis Get command
func (s *store) GetStruct(ctx context.Context, key string, target interface{}) (interface{}, error) {
	if target == nil {
		panic("target can't be nil.")
	}
	// s, err := connection.Client.Get(key).Result()
	// if err == nil{
	// 	err = json.Unmarshal([]byte(s), target)
	// }
	// if err == redis.Nil {return nil, err}
	// return target, err
	return target, nil
}

// Delete executes the redis Del command
func (s *store) Delete(ctx context.Context, key string) error {
	return nil
}
