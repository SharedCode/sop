package main

import (
	"context"

	"github.com/SharedCode/sop/btree"
	cas "github.com/SharedCode/sop/in_cas_s3/cassandra"
	"github.com/SharedCode/sop/in_cas_s3"
	"github.com/SharedCode/sop/in_cas_s3/redis"
)

var ctx = context.Background()
var cassConfig = cas.Config{
	ClusterHosts: []string{"172.17.0.2"},
	Keyspace:     "btree",
}

func init() {
	in_cas_s3.Initialize(cassConfig, redis.DefaultOptions())
}

func main() {

	repo := cas.NewStoreRepository()
	repo.Add(ctx, *btree.NewStoreInfo("", 4, true, true, "vid_1", "/Users/", ""))
	// conn.Session.ExecuteBatch()
}
