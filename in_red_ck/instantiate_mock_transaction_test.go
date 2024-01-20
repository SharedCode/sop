package in_red_ck

import (
	"time"
	"testing"

	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

// Global mock repositories will allow us to simulate repositories that persists state
// between transaction(commit).
var mockStoreRepository = cas.NewMockStoreRepository()
var mockRegistry = cas.NewMockRegistry(false)
var mockRedisCache = redis.NewMockClient()
var mockNodeBlobStore = cas.NewMockBlobStore()

func newMockTransaction(t testing.TB, forWriting bool, maxTime time.Duration) (Transaction, error) {
	t.Helper()
	twoPhase, err := newMockTwoPhaseCommitTransaction(t, forWriting, maxTime)
	if err != nil {
		return nil, err
	}
	return &singlePhaseTransaction{
		sopPhaseCommitTransaction: twoPhase,
	}, nil
}

func newMockTwoPhaseCommitTransaction(t testing.TB, forWriting bool, maxTime time.Duration) (TwoPhaseCommitTransaction, error) {
	t.Helper()
	if maxTime <= 0 {
		m := 15
		maxTime = time.Duration(m * int(time.Minute))
	}
	return &transaction{
		forWriting:      forWriting,
		maxTime:         maxTime,
		storeRepository: mockStoreRepository,
		registry:        mockRegistry,
		redisCache:      mockRedisCache,
		nodeBlobStore:   mockNodeBlobStore,
		logger:          newTransactionLogger(),
		phaseDone:       -1,
	}, nil
}
