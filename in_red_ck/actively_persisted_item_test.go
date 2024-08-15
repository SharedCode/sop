package in_red_ck

import (
	"testing"
	"time"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func Test_StreamingDataStoreRollbackShouldEraseTIDLogs(t *testing.T) {
	// Populate with good data.
	trans, _ := newMockTransactionWithLogging(t, ForWriting, -1)
	trans.Begin()

	so := sop.ConfigureStore("xyz", true, 8, "Streaming data", sop.BigData)
	sds, _ := NewBtree[string, string](ctx, so, trans)

	sds.Add(ctx, "fooVideo", "video content")
	trans.Commit(ctx)

	// Now, populate then rollback and validate TID logs are gone.
	trans, _ = newMockTransactionWithLogging(t, ForWriting, -1)
	trans.Begin()
	sds, _ = OpenBtree[string, string](ctx, "xyz", trans)
	sds.Add(ctx, "fooVideo2", "video content")

	tidLogs := trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).
		logger.logger.(*cas.MockTransactionLog).GetTIDLogs(
		trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).logger.transactionID)

	if tidLogs == nil {
		t.Error("failed pre Rollback, got nil, want valid logs")
	}

	trans.Rollback(ctx)

	gotTidLogs := trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).
		logger.logger.(*cas.MockTransactionLog).GetTIDLogs(
		trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).logger.transactionID)

	if gotTidLogs != nil {
		t.Errorf("failed Rollback, got %v, want nil", gotTidLogs)
	}
}

func Test_StreamingDataStoreAbandonedTransactionLogsGetCleaned(t *testing.T) {
	// Unwind time to yesterday.
	yesterday := time.Now().Add(time.Duration(-24 * time.Hour))
	cas.Now = func() time.Time { return yesterday }
	sop.Now = func() time.Time { return yesterday }
	Now = func() time.Time { return yesterday }

	trans, _ := newMockTransactionWithLogging(t, ForWriting, -1)
	trans.Begin()

	so := sop.ConfigureStore("xyz2", false, 8, "Streaming data", sop.BigData)
	b3, _ := NewBtree[PersonKey, Person](ctx, so, trans)

	pk, p := newPerson("joe", "shroeger", "male", "email", "phone")
	b3.Add(ctx, pk, p)

	trans.Commit(ctx)

	trans, _ = newMockTransactionWithLogging(t, ForWriting, -1)
	trans.Begin()

	b3, _ = OpenBtree[PersonKey, Person](ctx, "xyz2", trans)
	pk, p = newPerson("joe", "shroeger", "male", "email2", "phone2")
	b3.Update(ctx, pk, p)

	pt := trans.GetPhasedTransaction()
	twoPhaseTrans := pt.(*transaction)

	// GetOne should not get anything as uncommitted transaction is still ongoing or not expired.
	tid, _, _, _ := twoPhaseTrans.logger.logger.GetOne(ctx)
	if !tid.IsNil() {
		t.Errorf("Failed, got %v, want nil.", tid)
	}

	// Fast forward by a day to allow us to expire the uncommitted transaction.
	today := time.Now()
	cas.Now = func() time.Time { return today }
	sop.Now = func() time.Time { return today }
	Now = func() time.Time { return today }

	tid, _, _, _ = twoPhaseTrans.logger.logger.GetOne(ctx)
	if tid.IsNil() {
		t.Errorf("Failed, got nil, want valid Tid.")
	}

	if err := twoPhaseTrans.logger.processExpiredTransactionLogs(ctx, twoPhaseTrans); err != nil {
		t.Errorf("processExpiredTransactionLogs failed, got %v want nil.", err)
	}

	tid, _, _, _ = twoPhaseTrans.logger.logger.GetOne(ctx)
	if !tid.IsNil() {
		t.Errorf("Failed, got %v, want nil.", tid)
	}

	trans, _ = newMockTransactionWithLogging(t, ForReading, -1)
	trans.Begin()

	b3, _ = OpenBtree[PersonKey, Person](ctx, "xyz2", trans)

	b3.First(ctx)
	k := b3.GetCurrentKey()
	if k.Firstname == pk.Firstname && k.Lastname == pk.Lastname {
		if ok, _ := b3.Next(ctx); ok {
			t.Errorf("Failed, got true, want false.")
			return
		}
		return
	}
	t.Errorf("Failed, got %v, want %v.", k, pk)
}
