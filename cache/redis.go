package cache

import (
	"fmt"
	"encoding/json"

	"time"
//	"github.com/fatih/structs"
	"github.com/go-redis/redis"
)

type Options struct{
	Address string
	Password string	
	DB int
	DefaultDurationInSeconds int
}
func (opt *Options) GetDefaultDuration() time.Duration{
	return time.Duration(opt.DefaultDurationInSeconds)*time.Second
}

type Connection struct{
	Client *redis.Client
	Options Options
}


func DefaultOptions() Options {
	return Options{
		Address:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
		DefaultDurationInSeconds: 24*60*60,
	}
}

func NewClient(options Options) *Connection {
	client := redis.NewClient(&redis.Options{
		Addr:     options.Address,
		Password: options.Password,
		DB:       options.DB})

	c := Connection{
		Client : client,
		Options: options,
	}

	return &c
}

// Ping tests connectivity for redis (PONG should be returned)
func (connection *Connection) Ping() error {
	pong, err := connection.Client.Ping().Result()
	if err != nil {
		return err
	}
	fmt.Println(pong, err)
	// Output: PONG <nil>

	return nil
}

// Set executes the redis Set command
func (connection *Connection) Set(key string, value string, expiration time.Duration) error {
	return connection.Client.Set(key, value, expiration).Err()
}

// Get executes the redis Get command
func (connection *Connection) Get(key string) (string, error) {
	return connection.Client.Get(key).Result()
}

// SetStruct executes the redis Set command
func (connection *Connection) SetStruct(key string, value interface{}, expiration time.Duration) error {
	// serialize User object to JSON
	json, err := json.Marshal(value)
	if err != nil {
		return err
	}
	// SET object
	return connection.Client.Set(key, json, expiration).Err()
}

// GetStruct executes the redis Get command
func (connection *Connection) GetStruct(key string, target interface{}) (interface{}, error) {
	if target == nil{
		panic("target can't be nil.")
	}
	s, err := connection.Client.Get(key).Result()
	if err == nil{
		err = json.Unmarshal([]byte(s), target)
	}
	if err == redis.Nil {return nil, err}
	return target, err
}

// DeleteStruct executes the redis Del command
func (connection *Connection) DeleteStruct(key string) error {
	var r = connection.Client.Del(key)
	return r.Err()
}

// todo: potentially useful
// // SetStruct executes redis HMSet command
// func SetStruct(connection *Connection, key string, value interface{}) error {
// 	valueMap := structs.Map(value)
// 	return connection.Client.HMSet(key, valueMap).Err()
// }
// // SetStructField executes redis HSet
// func SetStructField(connection *Connection, key string, field string, value interface{}) (bool, error) {
// 	return connection.Client.HSet(key, field, value).Result()
// }

// // GetStruct executes redis HGetAll
// func GetStruct(connection *Connection, key string) (map[string]string, error) {
// 	return connection.Client.HGetAll(key).Result()
// }

// // GetStructField executes redis HGet
// func GetStructField(connection *Connection, key string, field string) (string, error) {
// 	return connection.Client.HGet(key, field).Result()
// }
