
package sop

import (
	"encoding/json"
	"io/ioutil"
    "github.com/SharedCode/sop/cache"
    cass "github.com/SharedCode/sop/store/cassandra"
)

// Configuration contains caching (redis) and backend store (e.g. Cassandra) host parameters.
type Configuration struct{
    RedisConfig cache.Options
    CassandraConfig cass.Config
}

// LoadConfiguration will read from a JSON file the configuration & load it into memory.
func LoadConfiguration(filename string) (Configuration, error) {
    bytes, err := ioutil.ReadFile(filename)
    if err != nil {
        return Configuration{}, err
    }

    var c Configuration
    err = json.Unmarshal(bytes, &c)
    if err != nil {
        return Configuration{}, err
    }

    return c, nil
}
