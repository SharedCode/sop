//go:build stress
// +build stress

package common

import (
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// Streaming data store heavy tests extracted for opt-in runs.

func TestStress_StreamingDataStoreRollbackShouldEraseTIDLogs(t *testing.T) {
	// Populate with good data.
	trans, _ := newMockTransactionWithLogging(t, sop.ForWriting, -1)
	trans.Begin()

	so := sop.ConfigureStore("xyz", true, 8, "Streaming data", sop.BigData, "")
	sds, _ := NewBtree[string, string](ctx, so, trans, nil)

	sds.Add(ctx, "fooVideo", "video content")
	trans.Commit(ctx)

	// Now, populate then rollback and validate TID logs are gone.
	trans, _ = newMockTransactionWithLogging(t, sop.ForWriting, -1)
	trans.Begin()
	sds, _ = OpenBtree[string, string](ctx, "xyz", trans, nil)
	sds.Add(ctx, "fooVideo2", "video content")

	tidLogs := trans.GetPhasedTransaction().(*Transaction).
		logger.TransactionLog.(*mocks.MockTransactionLog).GetTIDLogs(
		trans.GetPhasedTransaction().(*Transaction).logger.transactionID)

	if tidLogs == nil {
		t.Error("failed pre Rollback, got nil, want valid logs")
	}

	trans.Rollback(ctx)

	gotTidLogs := trans.GetPhasedTransaction().(*Transaction).
		logger.TransactionLog.(*mocks.MockTransactionLog).GetTIDLogs(
		trans.GetPhasedTransaction().(*Transaction).logger.transactionID)

	if gotTidLogs != nil {
		t.Errorf("failed Rollback, got %v, want nil", gotTidLogs)
	}
}
