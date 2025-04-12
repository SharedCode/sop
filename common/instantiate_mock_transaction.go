package common

import (
	"testing"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/common/mocks"
)

// Global mock repositories will allow us to simulate repositories that persists state
// between transaction(commit).
var mockStoreRepository = mocks.NewMockStoreRepository()
var mockRegistry = mocks.NewMockRegistry(false)
var mockRedisCache = mocks.NewMockClient()
var mockNodeBlobStore = mocks.NewMockBlobStore()

// newMockTransaction instantiates a mocked transaction, i.e. - it uses in-memory Repositories as backend, not Cassandra.
func newMockTransaction(t *testing.T, mode sop.TransactionMode, maxTime time.Duration) (sop.Transaction, error) {
	t.Helper()
	twoPhase, _ := newMockTwoPhaseCommitTransaction(t, mode, maxTime, false)
	return sop.NewTransaction(mode, twoPhase, maxTime, false)
}

// NewMockTransaction with logging turned on.
func newMockTransactionWithLogging(t *testing.T, mode sop.TransactionMode, maxTime time.Duration) (sop.Transaction, error) {
	t.Helper()
	twoPhase, _ := newMockTwoPhaseCommitTransaction(t, mode, maxTime, true)
	return sop.NewTransaction(mode, twoPhase, maxTime, true)
}

func newMockTwoPhaseCommitTransaction(t *testing.T, mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.TwoPhaseCommitTransaction, error) {
	t.Helper()
	if maxTime <= 0 {
		m := 15
		maxTime = time.Duration(m * int(time.Minute))
	}
	return &transaction{
		mode:            mode,
		maxTime:         maxTime,
		storeRepository: mockStoreRepository,
		registry:        mockRegistry,
		cache:           mockRedisCache,
		blobStore:       mockNodeBlobStore,
		logger:          newTransactionLogger(mocks.NewMockTransactionLog(), logging),
		phaseDone:       -1,
	}, nil
}
