package integration_tests

import (
	"testing"
	// "time"

	// "github.com/SharedCode/sop"
	// "github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func Test_TLog_FailOnFinalizeCommit(t *testing.T) {


	tl := cas.NewTransactionLog()
	tid, _, _ := tl.GetOne(ctx)
	
	if tid.IsNil() {
		t.Errorf("Failed, got nil Tid, want valid Tid.")
	}


	// // Unwind time to yesterday.
	// yesterday := time.Now().Add(time.Duration(-24*time.Hour))
	// cas.Now = func() time.Time { return yesterday }
	// sop.Now = func() time.Time { return yesterday }
	// in_red_ck.Now = func() time.Time { return yesterday }

	// trans, _ := in_red_ck.NewTransaction(true, -1, true)
	// trans.Begin()

	// b3, _ := in_red_ck.NewBtree[PersonKey, Person](ctx, "tlogtable", nodeSlotLength, false, false, false, "", trans)

	// pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	// b3.Add(ctx, pk, p)

	// trans.Commit(ctx)

	// trans, _ = in_red_ck.NewTransaction(true, -1, true)
	// trans.Begin()

	// b3, _ = in_red_ck.OpenBtree[PersonKey, Person](ctx, "tlogtable", trans)
	// pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	// b3.Update(ctx, pk, p)

	// pt := trans.GetPhasedTransaction()
	// pt.Phase1Commit(ctx)

	// // Fast forward by a day to allow us to expire the uncommitted transaction.
	// today := time.Now()
	// cas.Now = func() time.Time { return today }
	// sop.Now = func() time.Time { return today }
	// in_red_ck.Now = func() time.Time { return today }

	// tl := cas.NewTransactionLog()
	// tid, _, _ := tl.GetOne(ctx)
	
	// if tid.IsNil() {
	// 	t.Errorf("Failed, got nil Tid, want valid Tid.")
	// }

	// // if err := twoPhaseTrans.logger.processExpiredTransactionLogs(ctx, twoPhaseTrans); err != nil {
	// // 	t.Errorf("processExpiredTransactionLogs failed, got %v want nil.", err)
	// // }
}
