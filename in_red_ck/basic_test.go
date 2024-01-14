package in_red_ck

import (
	"context"
	"fmt"
	"testing"

	"github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

var cassConfig = cassandra.Config{
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
	Initialize(cassConfig, redisConfig)
}

var ctx = context.Background()

// TODO: more unit tests to follow. Unit tests are intentionally delayed to cover more grounds in
// implementation, as we still don't have enough developers. But before each release, e.g. alpha,
// beta, beta 2, production release, unit tests will get good coverage. See in-memory beta release,
// it has good coverage.
//
// Dev't is slightly tweaked tailored for one-man show or developer resources scarcity.
// a.k.a. - extreme RAD(Rapid Application Development). No choice actually, but if more developers
// come/volunteer then the approach will change.

func Test_TransactionStory_OpenVsNewBTree(t *testing.T) {
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooStore", 8, false, false, true, "", trans)
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Logf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if _, err := OpenBtree[int, string](ctx, "fooStore22", trans); err == nil {
		t.Logf("OpenBtree('fooStore', trans) failed, got nil want error.")
		trans.Rollback(ctx)
	}
}

func Test_TransactionStory_SingleBTree(t *testing.T) {
	// 1. Open a transaction
	// 2. Instantiate a BTree
	// 3. Do CRUD on BTree
	// 4. Commit Transaction
	trans, err := NewTransaction(true, -1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	trans.Begin()
	b3, _ := NewBtree[int, string](ctx, "fooStore", 8, false, false, true, "", trans)
	if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil {
		t.Errorf("Add(1, 'hello world') failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}

	if ok, err := b3.FindOne(ctx, 1, false); !ok || err != nil {
		t.Errorf("FindOne(1,false) failed, got(ok, err) = %v, %v, want = true, nil.", ok, err)
		trans.Rollback(ctx)
		return
	}
	if k, err := b3.GetCurrentKey(ctx); k != 1 || err != nil {
		t.Errorf("GetCurrentKey() failed, got = %v, %v, want = 1, nil.", k, err)
		trans.Rollback(ctx)
		return
	}
	if v, err := b3.GetCurrentValue(ctx); v != "hello world" || err != nil {
		t.Errorf("GetCurrentValue() failed, got = %v, %v, want = 1, nil.", v, err)
		trans.Rollback(ctx)
		return
	}
	t.Logf("Successfully added & found item with key 1.")
	// TODO: add more unit tests to exercise/verify commit's conflict detection, lightweight locking
	// and Nodes upserts, i.e. - save updated nodes, save removed nodes & save added nodes.
	if err := trans.Commit(ctx); err != nil {
		t.Errorf("Commit returned error, details: %v.", err)
	}
}

// This test took about 3 minutes from empty to finish in my laptop.
func Test_VolumeAddThenSearch(t *testing.T) {
	start := 9001
	end := 100000
	batchSize := 100

	t1, _ := NewTransaction(true, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)

	// Populating 90,000 items took about few minutes. Not bad considering I did not use Kafka queue
	// for scheduled batch deletes.
	for i := start; i <= end; i++ {
		pk, p := newPerson("jack", fmt.Sprintf("reepper%d", i), "male", "email very very long long long", "phone123")
		if ok, _ := b3.AddIfNotExist(ctx, pk, p); ok {
			t.Logf("%v inserted", pk)
		}
		if i % batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = NewTransaction(true, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
		}
	}

	// Search them all. Searching 90,000 items just took few seconds in my laptop.
	for i := start; i <= end; i++ {
		lname := fmt.Sprintf("reepper%d", i)
		pk, _ := newPerson("jack", lname, "male", "email very very long long long", "phone123")
		if found, err := b3.FindOne(ctx, pk, false); !found || err != nil {
			t.Error(err)
			t.Fail()
		}
		ci, _ := b3.GetCurrentItem(ctx)
		if ci.Value.Phone != "phone123" || ci.Key.Lastname != lname {
			t.Error(fmt.Errorf("Did not find the correct person with phone123 & lname %s", lname))
			t.Fail()
		}
		if i % batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = NewTransaction(false, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
		}
	}
}

func Test_VolumeDeletes(t *testing.T) {
	start := 9001
	end := 100000
	batchSize := 100

	t1, _ := NewTransaction(true, -1)
	t1.Begin()
	b3, _ := NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)

	// Populating 90,000 items took about few minutes, did not use Kafka based delete service.
	for i := start; i <= end; i++ {
		pk, _ := newPerson("jack", fmt.Sprintf("reepper%d", i), "male", "email very very long long long", "phone123")
		if ok, err := b3.Remove(ctx, pk); !ok || err != nil {
			if err != nil {
				t.Error(err)
			}
			// Ignore not found as item could had been deleted in previous run.
		}
		if i % batchSize == 0 {
			if err := t1.Commit(ctx); err != nil {
				t.Error(err)
				t.Fail()
			}
			t1, _ = NewTransaction(true, -1)
			t1.Begin()
			b3, _ = NewBtree[PersonKey, Person](ctx, "persondb", nodeSlotLength, false, false, false, "", t1)
		}
	}
}
