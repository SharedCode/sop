package main

import (
	"context"
	"cmp"
	"encoding/json"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs"
	"github.com/SharedCode/sop/in_red_cfs/fs"

	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/redis"
)

var ctx = context.TODO()

type PersonKey struct {
	FirstName string
	LastName string
}

type Person struct {
	PersonKey
	PhoneNo string
}

var cassConfig = cas.Config{
	ClusterHosts: []string{"localhost:9042"},
	Keyspace:     "btree",
}
var redisConfig = redis.Options{
	Address:                  "localhost:6379",
	Password:                 "", // no password set
	DB:                       0,  // use default DB
	DefaultDurationInSeconds: 24 * 60 * 60,
}

func init() {
	in_red_cfs.Initialize(cassConfig, redisConfig)
}


func Compare(a map[string]string, b map[string]string) int {
	i := cmp.Compare[string](a["LastName"], b["LastName"])
	if i != 0 {
		return i
	}
	return cmp.Compare[string](a["FirstName"], b["FirstName"])
}

func main() {

	ec := fs.ErasureCodingConfig{
		DataShardsCount:   2,
		ParityShardsCount: 1,
		BaseFolderPathsAcrossDrives: []string{
			"/Users/grecinto/sop_data/disk1",
			"/Users/grecinto/sop_data/disk2",
			"/Users/grecinto/sop_data/disk3",
		},
		RepairCorruptedShards: true,
	}
	trans, _ := in_red_cfs.NewTransactionWithEC(sop.ForWriting, -1, false, &ec)
	trans.Begin()
	b3, _ := in_red_cfs.NewBtreeWithEC[map[string]string, map[string]string](ctx, sop.StoreOptions{
		Name:                     "mapstring",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}, trans, Compare)

	k := PersonKey{FirstName: "joe"}
	p := Person{}
	p.FirstName = "joe"
	p.PhoneNo = "123"

	ba, _ := json.Marshal(k)
	ba2, _ := json.Marshal(p)
	var ki map[string]string
	var pi map[string]string
	json.Unmarshal(ba, &ki)
	json.Unmarshal(ba2, &pi)

	b3.Add(ctx, ki, pi)



}