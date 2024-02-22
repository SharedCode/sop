package in_red_ck

import (
	"testing"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

func Test_StreamingDataStoreRollbackShouldEraseTIDLogs(t *testing.T) {
	// Populate with good data.
	trans, _ := NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()

	sds, _ := NewBtree[string, string](ctx, sop.StoreOptions{
		Name:                         "xyz",
		SlotLength:                   8,
		IsUnique:                     true,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: true,
		IsValueDataGloballyCached:    false,
		LeafLoadBalancing:            false,
		Description:                  "Streaming data",
	}, trans)

	sds.Add(ctx, "fooVideo", "video content")
	trans.Commit(ctx)

	// Now, populate then rollback and validate TID logs are gone.
	trans, _ = NewMockTransactionWithLogging(t, true, -1)
	trans.Begin()
	sds, _ = OpenBtree[string, string](ctx, "xyz", trans)
	sds.Add(ctx, "fooVideo2", "video content")

	tidLogs := trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).
		logger.logger.(*cas.MockTransactionLog).GetTIDLogs(
		trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).logger.transactionID)

	if tidLogs == nil {
		t.Errorf("failed Rollback, got %v, want nil", tidLogs)
	}

	trans.Rollback(ctx)

	gotTidLogs := trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).
		logger.logger.(*cas.MockTransactionLog).GetTIDLogs(
		trans.(*singlePhaseTransaction).sopPhaseCommitTransaction.(*transaction).logger.transactionID)

	if gotTidLogs != nil {
		t.Errorf("failed Rollback, got %v, want nil", gotTidLogs)
	}
}
