package in_red_ck

import (
	"fmt"
	"testing"
	"time"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func Test_TLog_Rollback(t *testing.T) {
	trans, _ := NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, "tlogtable", nodeSlotLength, false, false, false, "", trans)

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
	pk, p = newPerson("joe", "shroeger", "male", "email", "phone")

	b3.FindOne(ctx, pk, false)
	v, _ := b3.GetCurrentValue(ctx)

	if v.Email != "email" {
		t.Errorf("Rollback did not restore person record, email got = %s, want = 'email'.", v.Email)
	}
	trans.Commit(ctx)
}

func Test_TLog_FailOnFinalizeCommit(t *testing.T) {
	// Unwind time to yesterday.
	yesterday := time.Now().Add(time.Duration(-24*time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	now = func() time.Time { return yesterday }

	trans, _ := NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	b3, _ := NewBtree[PersonKey, Person](ctx, "tlogtable", nodeSlotLength, false, false, false, "", trans)

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

	tid, _, _ := twoPhaseTrans.logger.logger.GetOne(ctx)
	if !tid.IsNil() {
		t.Errorf("Failed, got %v, want nil.", tid)
	}

	// Fast forward to allow us to get
	today := time.Now()
	cas.Now = func() time.Time { return today }
	sop.Now = func() time.Time { return today }
	now = func() time.Time { return today }
	
	tid, flogs, _ := twoPhaseTrans.logger.logger.GetOne(ctx)
	if tid.IsNil() {
		t.Errorf("Failed, got nil Tid, want valid Tid.")
	}
	for i := range flogs {
		fmt.Printf("commit function: %s\n", flogs[i].Key)
		fmt.Printf("commit function payload: %v\n", flogs[i].Value)
	}
}
