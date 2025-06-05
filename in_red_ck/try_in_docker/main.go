package main

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_ck"
	"github.com/SharedCode/sop/redis"
	"github.com/gocql/gocql"
)

var ctx = context.Background()
var cassConfig = cas.Config{
	ClusterHosts: []string{"172.17.0.2:9042"},
	Consistency:  gocql.Quorum,
}
var redisConfig = redis.Options{
	Address:  "172.17.0.1:6379",
	Password: "", // no password set
	DB:       0,  // use default DB
}

func main() {
	if err := in_red_ck.Initialize(cassConfig, redisConfig); err != nil {
		writeAndExit(err.Error())
	}
	so := sop.StoreOptions{
		Name:                         "foobar",
		SlotLength:                   4,
		IsValueDataInNodeSegment:     true,
		IsValueDataActivelyPersisted: true,
		IsValueDataGloballyCached:    true,
	}
	storeInfo := *sop.NewStoreInfo(so)
	storeInfo.RootNodeID = sop.NewUUID()
	repo := cas.NewStoreRepository(nil)
	sis, err := repo.Get(ctx, "foobar")
	if err != nil {
		writeAndExit("Cassandra repo Get failed, err: %v.", err)
	}
	if len(sis) == 0 {
		if err := repo.Add(ctx, storeInfo); err != nil {
			writeAndExit("Cassandra repo Add failed, err: %v.", err)
		}
	}

	registry := cas.NewRegistry()
	if err := registry.Add(ctx, []sop.RegistryPayload[sop.Handle]{
		sop.RegistryPayload[sop.Handle]{
			RegistryTable: storeInfo.RegistryTable,
			IDs:           []sop.Handle{sop.NewHandle(sop.NewUUID())},
		}}); err != nil {
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
