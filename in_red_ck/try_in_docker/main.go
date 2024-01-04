package main

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

var ctx = context.Background()
var cassConfig = cas.Config{
	ClusterHosts: []string{"172.17.0.2"},
}

func main() {
	if err := in_red_ck.Initialize(cassConfig, redis.DefaultOptions()); err != nil {
		writeAndExit(err.Error())
	}

	repo := cas.NewStoreRepository()
	if err := repo.Add(ctx, *btree.NewStoreInfo("foobar", 4, true, true, true, "")); err != nil {
		writeAndExit("Cassandra repo Add failed, err: %v.", err)
	}
	if s, err := repo.Get(ctx, "foobar"); err != nil {
		writeAndExit("Cassandra repo Get failed, err: %v.", err)
	} else {
		writeAndExit("Store got: %v.", s)
	}
	// conn.Session.ExecuteBatch()

	writeAndExit("Our cool app completed! -from docker.")
}

func writeAndExit(template string, args ...interface{}) {
	panic(fmt.Sprintf(template, args...))
}
