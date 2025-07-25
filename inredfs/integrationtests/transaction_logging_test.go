package integrationtests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
)

func MultipleExpiredTransCleanup(t *testing.T) {
	ctx := context.Background()
	inredfs.RemoveBtree(ctx, dataPath, "ztab1")

	// Seed with good records.
	yesterday := time.Now().Add(time.Duration(-48 * time.Hour))
	sop.Now = func() time.Time { return yesterday }

	to, _ := inredfs.NewTransactionOptions(dataPath, sop.ForWriting, -1, fs.MinimumModValue)
	trans, _ := inredfs.NewTransaction(ctx, to)
	trans.Begin()

	b3, _ := inredfs.NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "ztab1",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}, trans, Compare)

	for i := 0; i < 50; i++ {
		pk, p := newPerson("joe", fmt.Sprintf("krueger%d", i), "male", "email", "phone")
		b3.Add(ctx, pk, p)
	}

	trans.Commit(ctx)

	// Create & leave transaction 1 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-47 * time.Hour))
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredfs.NewTransaction(ctx, to)
	trans.Begin()

	b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	pk, p := newPerson("joe", "krueger77", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	// Create & leave transaction 2 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-46 * time.Hour))
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredfs.NewTransaction(ctx, to)
	trans.Begin()

	b3, _ = inredfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	pk, p = newPerson("joe", "krueger47", "male", "email2", "phone")
	b3.Update(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	yesterday = time.Now()
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredfs.NewTransaction(ctx, to)

	// Cleanup should be launched from this call.
	trans.Begin()
}

func Cleanup(t *testing.T) {
	ctx := context.Background()
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	sop.Now = func() time.Time { return yesterday }

	to2, _ := inredfs.NewTransactionOptions(dataPath, sop.ForReading, -1, fs.MinimumModValue)
	trans, _ := inredfs.NewTransaction(ctx, to2)
	trans.Begin()
	_, _ = inredfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	trans.Commit(ctx)

	yesterday = time.Now().Add(-time.Duration(23*time.Hour + 54*time.Minute))
	sop.Now = func() time.Time { return yesterday }

	trans, _ = inredfs.NewTransaction(ctx, to2)
	trans.Begin()
	_, _ = inredfs.OpenBtree[PersonKey, Person](ctx, "ztab1", trans, Compare)
	trans.Commit(ctx)
}
