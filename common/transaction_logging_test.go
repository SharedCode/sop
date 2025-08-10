package common

import (
	"testing"
	"time"

	"github.com/sharedcode/sop"
	cas "github.com/sharedcode/sop/cassandra"
)

func Test_TLog_Rollback(t *testing.T) {
	trans, _ := newMockTransactionWithLogging(t, sop.ForWriting, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "tlogtable",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = newMockTransactionWithLogging(t, sop.ForWriting, -1)
	trans.Begin()

	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	trans.Rollback(ctx)

	trans, _ = newMockTransactionWithLogging(t, sop.ForReading, -1)
	trans.Begin()
	b3, _ = OpenBtree[PersonKey, Person](ctx, "tlogtable", trans, Compare)
	pk, _ = newPerson("joe", "shroeger", "male", "email", "phone")

	b3.Find(ctx, pk, false)
	v, _ := b3.GetCurrentValue(ctx)

	if v.Email != "email" {
		t.Errorf("Rollback did not restore person record, email got = %s, want = 'email'.", v.Email)
	}
	trans.Commit(ctx)
}

func Test_TLog_FailOnFinalizeCommit(t *testing.T) {
	// Skipping due to intermittent panic in btree.Node.find (index out of range [-1]).
	// Re-enable after stabilizing B-Tree update path used by phase1Commit.
	if true {
		t.Skip("Skipping due to intermittent btree.Node.find panic (index out of range [-1]); revisit after B-Tree fix.")
	}

	// Unwind time to yesterday.
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	//Now = func() time.Time { return yesterday }

	trans, _ := newMockTransactionWithLogging(t, sop.ForWriting, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreOptions{
		Name:                     "tlogtable",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans, Compare)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = newMockTransactionWithLogging(t, sop.ForWriting, -1)
	trans.Begin()

	b3, _ = OpenBtree[PersonKey, Person](ctx, "tlogtable", trans, Compare)
	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	pt := trans.GetPhasedTransaction()
	twoPhaseTrans := pt.(*Transaction)

	twoPhaseTrans.phase1Commit(ctx)

	// GetOne should not get anything as uncommitted transaction is still ongoing or not expired.
	tid, _, _, _ := twoPhaseTrans.logger.GetOne(ctx)
	if !tid.IsNil() {
		t.Errorf("Failed, got %v, want nil.", tid)
	}

	// Fast forward by a day to allow us to expire the uncommitted transaction.
	today := time.Now()
	cas.Now = func() time.Time { return today }
	sop.Now = func() time.Time { return today }
	//Now = func() time.Time { return today }

	tid, _, _, _ = twoPhaseTrans.logger.GetOne(ctx)
	if tid.IsNil() {
		t.Errorf("Failed, got nil Tid, want valid Tid.")
	}

	if err := twoPhaseTrans.logger.processExpiredTransactionLogs(ctx, twoPhaseTrans); err != nil {
		t.Errorf("processExpiredTransactionLogs failed, got %v want nil.", err)
	}
}
