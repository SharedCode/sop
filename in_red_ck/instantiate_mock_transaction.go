package in_red_ck

import(
	"time"

	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
	"github.com/SharedCode/sop/in_red_ck/redis"
)

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
		storeRepository: cas.NewMockStoreRepository(),
		registry:        cas.NewMockRegistry(),
		redisCache:      redis.NewMockClient(),
		nodeBlobStore:   cas.NewMockBlobStore(),
		logger:          newTransactionLogger(),
		phaseDone:       -1,
	}, nil
}
