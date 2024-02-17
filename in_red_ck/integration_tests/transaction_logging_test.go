package integration_tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func Test_MultipleExpiredTransCleanup(t *testing.T) {
	in_red_ck.RemoveBtree(ctx, "ztab1")

	// Seed with good records.
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	in_red_ck.Now = func() time.Time { return yesterday }

	trans, _ := in_red_ck.NewTransaction(true, -1, true)
	trans.Begin()

	b3, _ := in_red_ck.NewBtree[PersonKey, Person](ctx, "ztab1", 8, false, true, false, "", trans)

	for i := 0; i < 50; i++ {
		pk, p := newPerson("joe", fmt.Sprintf("krueger%d", i), "male", "email", "phone")
		b3.Add(ctx, pk, p)
		}

	trans.Commit(ctx)

	// Create & leave transaction 1 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-23 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	in_red_ck.Now = func() time.Time { return yesterday }

	trans, _ = in_red_ck.NewTransaction(true, -1, true)
	trans.Begin()

	b3, _ = in_red_ck.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	pk, p := newPerson("joe", "krueger77", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	// Create & leave transaction 2 resources for cleanup.
	yesterday = time.Now().Add(time.Duration(-22 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	in_red_ck.Now = func() time.Time { return yesterday }

	trans, _ = in_red_ck.NewTransaction(true, -1, true)
	trans.Begin()

	b3, _ = in_red_ck.OpenBtree[PersonKey, Person](ctx, "ztab1", trans)
	pk, p = newPerson("joe", "krueger47", "male", "email2", "phone")
	b3.Update(ctx, pk, p)

	trans.GetPhasedTransaction().Phase1Commit(ctx)

	yesterday = time.Now()
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	in_red_ck.Now = func() time.Time { return yesterday }

	trans, _ = in_red_ck.NewTransaction(true, -1, true)

	// Cleanup should be launched from this call.
	trans.Begin()

}

func TestCleanup(t *testing.T) {
	yesterday := time.Now()
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	in_red_ck.Now = func() time.Time { return yesterday }

	trans, _ := in_red_ck.NewTransaction(true, -1, true)

	// Cleanup should be launched from this call.
	trans.Begin()

	trans.Commit(ctx)
}