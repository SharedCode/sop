package fs

import (
	"encoding/json"
	"testing"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

type payload struct {
	Abc string
	Xyc int
}

var uuid2, _ = sop.ParseUUID("6ba7b810-9dad-11d1-80b4-00c04fd430c9")

func TestTransactionLogAdd(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	ba, _ := json.Marshal(payload{
		Abc: "abc", Xyc: 123,
	})
	err := tl.Add(ctx, uuid, 1, ba)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
}

func TestTransactionLogGetOne(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	// ageLimit = 0
	uid, hour, tlogdata, err := tl.GetOne(ctx)
	if uid.IsNil() {
		return
	}
	if err != nil {
		t.Errorf("error got on tl.GetOne, details: %v", err)
	}
	if uid != uuid {
		t.Errorf("expected: %v, got: %v", uuid, uid)
	}
	if tlogdata[0].Key != 1 {
		t.Errorf("log data key expected: %d, got: %d", 1, tlogdata[0].Key)
	}
	var p payload
	json.Unmarshal(tlogdata[0].Value, &p)

	if p.Abc != "abc" {
		t.Errorf("Abc expected: abc, got: %s", p.Abc)
	}

	uid, tlogdata2, err := tl.GetLogsDetails(ctx, hour)

	if err != nil {
		t.Errorf("error got on tl.GetOne, details: %v", err)
	}
	if uid != uuid {
		t.Errorf("expected: %v, got: %v", uuid, uid)
	}
	if tlogdata2[0].Key != 1 {
		t.Errorf("log data key expected: %d, got: %d", 1, tlogdata2[0].Key)
	}
	json.Unmarshal(tlogdata2[0].Value, &p)

	if p.Abc != "abc" {
		t.Errorf("Abc expected: abc, got: %s", p.Abc)
	}
}

func TestTransactionLogAdd2(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	ba, _ := json.Marshal(payload{
		Abc: "abc", Xyc: 123,
	})
	ba2, _ := json.Marshal(payload{
		Abc: "xyz", Xyc: 456,
	})
	err := tl.Add(ctx, uuid2, 1, ba)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
	err = tl.Add(ctx, uuid2, 2, ba2)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
}

func TestTransactionLogGetOne2(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	// ageLimit = 0
	uid, _, tlogdata, err := tl.GetOne(ctx)
	if uid.IsNil() {
		return
	}
	if err != nil {
		t.Errorf("error got on tl.GetOne, details: %v", err)
	}
	if uid != uuid2 {
		t.Errorf("expected: %v, got: %v", uuid, uid)
	}
	if tlogdata[0].Key != 1 {
		t.Errorf("log data key expected: %d, got: %d", 1, tlogdata[0].Key)
	}
	var p payload
	json.Unmarshal(tlogdata[0].Value, &p)

	if p.Abc != "abc" {
		t.Errorf("Abc expected: abc, got: %s", p.Abc)
	}

	if tlogdata[1].Key != 2 {
		t.Errorf("log data key expected: %d, got: %d", 2, tlogdata[0].Key)
	}

	json.Unmarshal(tlogdata[1].Value, &p)

	if p.Abc != "xyz" {
		t.Errorf("Abc expected: xyz, got: %s", p.Abc)
	}
}

func TestTransactionLogRemove2(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	err := tl.Remove(ctx, uuid2)
	if err != nil {
		t.Errorf("Remove err: %v", err)
	}
}

func TestTransactionLogAddRemove(t *testing.T) {
	l2cache := redis.NewClient()
	rt, _ := NewReplicationTracker(ctx, []string{"/Users/grecinto/sop_data/"}, false, l2cache)
	tl := NewTransactionLog(l2cache, rt)
	ba, _ := json.Marshal(payload{
		Abc: "abc", Xyc: 123,
	})
	err := tl.Add(ctx, uuid, 1, ba)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
	err = tl.Remove(ctx, uuid)
	if err != nil {
		t.Errorf("error got on tl.Add, details: %v", err)
	}
}
