package main

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
	"github.com/gocql/gocql"
)

var ctx = context.Background()
var cassConfig = cas.Config{
	ClusterHosts: []string{"172.17.0.2"},
	Consistency: gocql.Quorum,
}

func main() {
	redisConfig := redis.Options {
		Address:                  "USLC02SGDPXG8WP:6379",
		Password:                 "", // no password set
		DB:                       0,  // use default DB
		DefaultDurationInSeconds: 24 * 60 * 60,
	}
	if err := in_red_ck.Initialize(cassConfig, redisConfig); err != nil {
		writeAndExit(err.Error())
	}

	storeInfo := *btree.NewStoreInfo("foobar", 4, true, true, true, "")
	storeInfo.RootNodeId = btree.NewUUID()
	repo := cas.NewStoreRepository()
	sis, err := repo.Get(ctx, "foobar")
	if err != nil {
		writeAndExit("Cassandra repo Get failed, err: %v.", err)
	}
	if len(sis) == 0 {
		if err := repo.Add(ctx, storeInfo); err != nil {
			writeAndExit("Cassandra repo Add failed, err: %v.", err)
		}
	}

	registry,_ := cas.NewRegistry(redis.NewClient())
	if err := registry.Add(ctx, cas.RegistryPayload[sop.Handle]{
		RegistryTable: storeInfo.RegistryTable,
		IDs: []sop.Handle{ sop.NewHandle(btree.NewUUID()) },
	}); err != nil {
		writeAndExit("Cassandra registry Add failed, err: %v.", err)
	}

	// if err := repo.Remove(ctx, "foobar"); err != nil {
	// 	writeAndExit("Cassandra repo Remove failed, err: %v.", err)
	// }
	writeAndExit("Our cool app completed! -from docker.")
}

func writeAndExit(template string, args ...interface{}) {
	panic(fmt.Sprintf(template, args...))
}
