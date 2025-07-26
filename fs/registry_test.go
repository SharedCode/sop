package fs

import (
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/redis"
)

func init() {
	var redisConfig = redis.Options{
		Address:  "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	}
	redis.OpenConnection(redisConfig)
}

var uuid, _ = sop.ParseUUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
var hashMod = MinimumModValue

func TestRegistryAddThenRead(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	r := NewRegistry(true, hashMod, rt, l2cache)

	h := sop.NewHandle(uuid)

	if err := r.Add(ctx, []sop.RegistryPayload[sop.Handle]{
		sop.RegistryPayload[sop.Handle]{
			RegistryTable: "regtest",
			BlobTable:     "regtest",
			IDs:           []sop.Handle{h},
		}}); err != nil {
		t.Error(err.Error())
	}

	if h2, err := r.Get(ctx, []sop.RegistryPayload[sop.UUID]{
		sop.RegistryPayload[sop.UUID]{
			RegistryTable: "regtest",
			BlobTable:     "regtest",
			IDs:           []sop.UUID{h.LogicalID},
		}}); err != nil {
		t.Error(err.Error())
	} else {
		if h2[0].IDs[0].LogicalID != h.LogicalID {
			t.Errorf("Expected %v, got %v", h.LogicalID, h2[0].IDs[0].LogicalID)
		}
	}
}
