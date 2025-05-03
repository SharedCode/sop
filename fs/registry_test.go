package fs

import (
	"testing"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

func init() {
	var redisConfig = redis.Options{
		Address:                  "localhost:6379",
		Password:                 "", // no password set
		DB:                       0,  // use default DB
		DefaultDurationInSeconds: 24 * 60 * 60,
	}
	redis.OpenConnection(redisConfig)
}

func TestRegistry(t *testing.T) {
	r := NewRegistry(true, SmallModValue, NewReplicationTracker([]string{"/Users/grecinto/sop_data/"}, false), redis.NewClient(), false)
	h := sop.NewHandle(sop.NewUUID())

	if err := r.Add(ctx, sop.RegistryPayload[sop.Handle]{
		RegistryTable: "regtest",
		BlobTable: "regtest",
		IDs: []sop.Handle{h},
	}); err != nil {
		t.Error(err.Error())
	}

	if h2, err :=r.Get(ctx, sop.RegistryPayload[sop.UUID]{
		RegistryTable: "regtest",
		BlobTable: "regtest",
		IDs: []sop.UUID{h.LogicalID},
	}); err != nil {
		t.Error(err.Error())
	} else {
		if h2[0].IDs[0].LogicalID != h.LogicalID {
			t.Errorf("Expected %v, got %v", h.LogicalID, h2[0].IDs[0].LogicalID)
		}
	}
}
