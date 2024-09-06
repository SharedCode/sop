package integration_tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cs3"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func MultipleExpiredTransCleanup(t *testing.T) {
	in_red_cs3.RemoveBtree(ctx, "ztab1")

	// Seed with good records.
	yesterday := time.Now().Add(time.Duration(-48 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	cas.Now = func() time.Time { return yesterday }

	trans, _ := in_red_cs3.NewTransaction(ctx, sop.ForWriting, -1, true, region)
	trans.Begin()

	b3, _ := in_red_cs3.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "ztab1",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        false,
		Description:              "",
		BlobStoreBaseFolderPath:  dataPath,
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

	trans, _ = in_red_cs3.NewTransaction(ctx, sop.ForWriting, -1, true, region)
	trans.Begin()

	b3, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	pk, p := newPerson("joe", "krueger77", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	// Create & leave transaction 2 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-46 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(ctx, sop.ForWriting, -1, true, region)
	trans.Begin()

	b3, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	pk, p = newPerson("joe", "krueger47", "male", "email2", "phone")
	b3.Update(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	yesterday = time.Now()
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(ctx, sop.ForWriting, -1, true, region)

	// Cleanup should be launched from this call.
	trans.Begin()

}

func Cleanup(t *testing.T) {
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ := in_red_cs3.NewTransaction(ctx, sop.ForReading, -1, true, region)
	trans.Begin()
	_, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	trans.Commit(ctx)

	yesterday = time.Now().Add(-time.Duration(23*time.Hour + 54*time.Minute))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = in_red_cs3.NewTransaction(ctx, sop.ForReading, -1, true, region)
	trans.Begin()
	_, _ = in_red_cs3.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	trans.Commit(ctx)
}
