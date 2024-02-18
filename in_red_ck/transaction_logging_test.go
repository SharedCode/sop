package in_red_ck

import (
	"testing"
	"time"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func Test_TLog_Rollback(t *testing.T) {
	trans, _ := NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreInfo{
		Name:                     "tlogtable",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	trans.Rollback(ctx)

	trans, _ = NewMockTransactionWithLogging(t, false, -1)
	trans.Begin()
	b3, _ = OpenBtree[PersonKey, Person](ctx, "tlogtable", trans)
	pk, _ = newPerson("joe", "shroeger", "male", "email", "phone")

	b3.FindOne(ctx, pk, false)
	v, _ := b3.GetCurrentValue(ctx)

	if v.Email != "email" {
		t.Errorf("Rollback did not restore person record, email got = %s, want = 'email'.", v.Email)
	}
	trans.Commit(ctx)
}

func Test_TLog_FailOnFinalizeCommit(t *testing.T) {
	// Unwind time to yesterday.
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	Now = func() time.Time { return yesterday }

	trans, _ := NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, sop.StoreInfo{
		Name:                     "tlogtable",
		SlotLength:               nodeSlotLength,
		IsUnique:                 false,
		IsValueDataInNodeSegment: false,
		LeafLoadBalancing:        false,
		Description:              "",
	}, trans)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ = OpenBtree[PersonKey, Person](ctx, "tlogtable", trans)
	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	pt := trans.GetPhasedTransaction()
	twoPhaseTrans := pt.(*transaction)

	twoPhaseTrans.phase1Commit(ctx)

	// GetOne should not get anything as uncommitted transaction is still ongoing or not expired.
	tid, _, _, _ := twoPhaseTrans.logger.logger.GetOne(ctx)
	if !cas.IsNil(tid) {
		t.Errorf("Failed, got %v, want nil.", tid)
	}

	// Fast forward by a day to allow us to expire the uncommitted transaction.
	today := time.Now()
	cas.Now = func() time.Time { return today }
	sop.Now = func() time.Time { return today }
	Now = func() time.Time { return today }

	tid, _, _, _ = twoPhaseTrans.logger.logger.GetOne(ctx)
	if cas.IsNil(tid) {
		t.Errorf("Failed, got nil Tid, want valid Tid.")
	}

	if err := twoPhaseTrans.logger.processExpiredTransactionLogs(ctx, twoPhaseTrans); err != nil {
		t.Errorf("processExpiredTransactionLogs failed, got %v want nil.", err)
	}
}
