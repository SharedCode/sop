package in_red_ck

import(
	"time"

	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

// Global mock repositories will allow us to simulate repositories that persists state
// between transaction(commit).
var storeRepository = cas.NewMockStoreRepository()
var registry =       cas.NewMockRegistry()
var redisCache =      redis.NewMockClient()
var nodeBlobStore =   cas.NewMockBlobStore()

func newMockTransaction(forWriting bool, maxTime time.Duration) (Transaction, error) {
	twoPhase, err := newMockTwoPhaseCommitTransaction(forWriting, maxTime)
	if err != nil {
		return nil, err
	}
	return &singlePhaseTransaction{
		sopPhaseCommitTransaction: twoPhase,
	}, nil
}

func newMockTwoPhaseCommitTransaction(forWriting bool, maxTime time.Duration) (TwoPhaseCommitTransaction, error) {
	if maxTime <= 0 {
		m := 15
		maxTime = time.Duration(m * int(time.Minute))
	}
	return &transaction{
		forWriting: forWriting,
		maxTime:    maxTime,
		storeRepository: storeRepository,
		registry:        registry,
		redisCache:      redisCache,
		nodeBlobStore:   nodeBlobStore,
		logger:          newTransactionLogger(),
		phaseDone:       -1,
	}, nil
}
