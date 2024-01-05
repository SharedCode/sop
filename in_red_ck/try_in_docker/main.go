package main

import (
	"context"
	"fmt"

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
	if err := in_red_ck.Initialize(cassConfig, redis.DefaultOptions()); err != nil {
		writeAndExit(err.Error())
	}

	storeInfo := *btree.NewStoreInfo("foobar", 4, true, true, true, "")
	storeInfo.RootNodeId = btree.NewUUID()
	repo := cas.NewStoreRepository()
	if err := repo.Add(ctx, storeInfo); err != nil {
		writeAndExit("Cassandra repo Add failed, err: %v.", err)
	}
	if _, err := repo.Get(ctx, "foobar"); err != nil {
		writeAndExit("Cassandra repo Get failed, err: %v.", err)
	}
	if err := repo.Remove(ctx, "foobar"); err != nil {
		writeAndExit("Cassandra repo Remove failed, err: %v.", err)
	}
	writeAndExit("Our cool app completed! -from docker.")
}

func writeAndExit(template string, args ...interface{}) {
	panic(fmt.Sprintf(template, args...))
}
