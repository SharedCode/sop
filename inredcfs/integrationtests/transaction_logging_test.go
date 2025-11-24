//go:build integration
// +build integration

package integrationtests

import (
	"fmt"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	cas "github.com/sharedcode/sop/cassandra"
	"github.com/sharedcode/sop/inredcfs"
)

func MultipleExpiredTransCleanup(t *testing.T) {
	inredcfs.RemoveBtree(ctx, "ztab1")

	// Seed with good records.
	yesterday := time.Now().Add(time.Duration(-48 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ := inredcfs.NewTransaction(sop.ForWriting, -1, true)
	trans.Begin(ctx)

	b3, _ := inredcfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "ztab1",
		SlotLength:               8,
		IsUnique:                 false,
		IsValueDataInNodeSegment: true,
		LeafLoadBalancing:        false,
		Description:              "",
		BlobStoreBaseFolderPath:  dataPath,
	}, trans, Compare)

	for i := 0; i < 50; i++ {
		pk, p := newPerson("joe", fmt.Sprintf("krueger%d", i), "male", "email", "phone")
		b3.Add(ctx, pk, p)
	}

	trans.Commit(ctx)

	// Create & leave transaction 1 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-47 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredcfs.NewTransaction(sop.ForWriting, -1, true)
	trans.Begin(ctx)

	b3, _ = inredcfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	pk, p := newPerson("joe", "krueger77", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	// Create & leave transaction 2 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-46 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredcfs.NewTransaction(sop.ForWriting, -1, true)
	trans.Begin(ctx)

	b3, _ = inredcfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	pk, p = newPerson("joe", "krueger47", "male", "email2", "phone")
	b3.Update(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	yesterday = time.Now()
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredcfs.NewTransaction(sop.ForWriting, -1, true)

	// Cleanup should be launched from this call.
	trans.Begin(ctx)

}

func Cleanup(t *testing.T) {
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ := inredcfs.NewTransaction(sop.ForReading, -1, true)
	trans.Begin(ctx)
	_, _ = inredcfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	trans.Commit(ctx)

	yesterday = time.Now().Add(-time.Duration(23*time.Hour + 54*time.Minute))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredcfs.NewTransaction(sop.ForReading, -1, true)
	trans.Begin(ctx)
	_, _ = inredcfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	trans.Commit(ctx)
}
