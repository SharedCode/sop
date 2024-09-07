package integration_tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cs3"
)

func Test_MultipleExpiredTransCleanup(t *testing.T) {

	// Detect if BTree exists or not by trying to "open" it.
	tableName := "ztab1"
	trans, err  := in_red_cs3.NewTransaction(s3Client, sop.ForWriting, -1, true, region)
	if err != nil {
		t.Error(err)
	}
	trans.Begin()
	if _, err := in_red_cs3.OpenBtree[PersonKey, Person](ctx, tableName, trans); err == nil {
		trans.Commit(ctx)
		// Delete contents and the B-Tree itself from the backend.
		if err := in_red_cs3.RemoveBtree[PersonKey, Person](ctx, s3Client, region, tableName); err != nil {
			t.Error(err)
		}
	} else {
		trans.Rollback(ctx)
	}

	// Seed with good records.
	yesterday := time.Now().Add(time.Duration(-48 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	cas.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(s3Client, sop.ForWriting, -1, true, region)
	trans.Begin()

	b3, _ := in_red_cs3.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "ztab1",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans)

	for i := 0; i < 50; i++ {
		pk, p := newPerson("joe", fmt.Sprintf("krueger%d", i), "male", "email", "phone")
		b3.Add(ctx, pk, p)
	}

	trans.Commit(ctx)

	// Create & leave transaction 1 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-47 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(s3Client, sop.ForWriting, -1, true, region)
	trans.Begin()

	b3, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	pk, p := newPerson("joe", "krueger77", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	// Create & leave transaction 2 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-46 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(s3Client, sop.ForWriting, -1, true, region)
	trans.Begin()

	b3, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	pk, p = newPerson("joe", "krueger47", "male", "email2", "phone")
	b3.Update(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	yesterday = time.Now()
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(s3Client, sop.ForWriting, -1, true, region)

	// Cleanup should be launched from this call.
	trans.Begin()

}

func Test_Cleanup(t *testing.T) {
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ := in_red_cs3.NewTransaction(s3Client, sop.ForReading, -1, true, region)
	trans.Begin()
	_, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	trans.Commit(ctx)

	yesterday = time.Now().Add(-time.Duration(23*time.Hour + 54*time.Minute))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(s3Client, sop.ForReading, -1, true, region)
	trans.Begin()
	_, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	trans.Commit(ctx)
}
