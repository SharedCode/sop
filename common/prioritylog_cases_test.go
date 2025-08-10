package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common/mocks"
)

// stubPriorityLog implements sop.TransactionPriorityLog for deterministic tests.
type stubPriorityLog struct {
	batch           []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]
	writeBackupErr  map[string]error
	removeErr       map[string]error
	removeBackupHit map[string]int
}

func (s *stubPriorityLog) IsEnabled() bool                                             { return true }
func (s *stubPriorityLog) Add(ctx context.Context, tid sop.UUID, payload []byte) error { return nil }
func (s *stubPriorityLog) Remove(ctx context.Context, tid sop.UUID) error {
	if err, ok := s.removeErr[tid.String()]; ok {
		return err
	}
	return nil
}
func (s *stubPriorityLog) Get(ctx context.Context, tid sop.UUID) ([]sop.RegistryPayload[sop.Handle], error) {
	for _, kv := range s.batch {
		if kv.Key.Compare(tid) == 0 {
			return kv.Value, nil
		}
	}
	return nil, nil
}
func (s *stubPriorityLog) GetBatch(ctx context.Context, batchSize int) ([]sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]], error) {
	return s.batch, nil
}
func (s *stubPriorityLog) LogCommitChanges(ctx context.Context, stores []sop.StoreInfo, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) error {
	return nil
}
func (s *stubPriorityLog) WriteBackup(ctx context.Context, tid sop.UUID, payload []byte) error {
	if err, ok := s.writeBackupErr[tid.String()]; ok {
		return err
	}
	return nil
}
func (s *stubPriorityLog) RemoveBackup(ctx context.Context, tid sop.UUID) error {
	if s.removeBackupHit == nil {
		s.removeBackupHit = make(map[string]int)
	}
	s.removeBackupHit[tid.String()]++
	return nil
}

// stubTLog implements sop.TransactionLog and returns our stubPriorityLog.
type stubTLog struct{ pl *stubPriorityLog }

func (l stubTLog) PriorityLog() sop.TransactionPriorityLog { return l.pl }
func (l stubTLog) Add(ctx context.Context, tid sop.UUID, commitFunction int, payload []byte) error {
	return nil
}
func (l stubTLog) Remove(ctx context.Context, tid sop.UUID) error { return nil }
func (l stubTLog) GetOne(ctx context.Context) (sop.UUID, string, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, "", nil, nil
}
func (l stubTLog) GetOneOfHour(ctx context.Context, hour string) (sop.UUID, []sop.KeyValuePair[int, []byte], error) {
	return sop.NilUUID, nil, nil
}
func (l stubTLog) NewUUID() sop.UUID { return sop.NewUUID() }

func Test_TransactionLogger_DoPriorityRollbacks_Cases(t *testing.T) {
	ctx := context.Background()

	mkHandle := func(id sop.UUID, ver int32) sop.Handle {
		h := sop.NewHandle(id)
		h.Version = ver
		return h
	}

	// Common store payload factory
	makePayload := func(rt, bt string, ids []sop.Handle) []sop.RegistryPayload[sop.Handle] {
		return []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: rt, BlobTable: bt, IDs: ids},
		}
	}

	t.Run("consumes_one_success", func(t *testing.T) {
		// Mocks
		redis := mocks.NewMockClient()
		reg := mocks.NewMockRegistry(false)
		tx := &Transaction{l2Cache: redis, registry: reg}
		// Create one tid with one handle, version aligned
		tid := sop.NewUUID()
		lid := sop.NewUUID()
		uh := mkHandle(lid, 3)
		// Seed current registry with same version to satisfy check
		_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "rt", IDs: []sop.Handle{mkHandle(lid, 3)}},
		})
		pl := &stubPriorityLog{batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: tid, Value: makePayload("rt", "bt", []sop.Handle{uh})}}}
		tl := newTransactionLogger(stubTLog{pl: pl}, true)

		consumed, err := tl.doPriorityRollbacks(ctx, tx)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !consumed {
			t.Fatalf("expected consumed=true")
		}
		// Backup should be removed
		if pl.removeBackupHit[tid.String()] == 0 {
			t.Fatalf("expected RemoveBackup to be called")
		}
	})

	t.Run("write_backup_error_continue", func(t *testing.T) {
		redis := mocks.NewMockClient()
		reg := mocks.NewMockRegistry(false)
		tx := &Transaction{l2Cache: redis, registry: reg}
		tid := sop.NewUUID()
		lid := sop.NewUUID()
		// Seed registry to avoid version error; lock path should succeed
		_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "rt", IDs: []sop.Handle{mkHandle(lid, 1)}},
		})
		pl := &stubPriorityLog{batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: tid, Value: makePayload("rt", "bt", []sop.Handle{mkHandle(lid, 1)})}}, writeBackupErr: map[string]error{tid.String(): fmt.Errorf("wb err")}}
		tl := newTransactionLogger(stubTLog{pl: pl}, true)
		consumed, err := tl.doPriorityRollbacks(ctx, tx)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !consumed {
			t.Fatalf("expected consumed=true due to batch present")
		}
	})

	t.Run("remove_error_continue", func(t *testing.T) {
		redis := mocks.NewMockClient()
		reg := mocks.NewMockRegistry(false)
		tx := &Transaction{l2Cache: redis, registry: reg}
		tid := sop.NewUUID()
		lid := sop.NewUUID()
		_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "rt", IDs: []sop.Handle{mkHandle(lid, 1)}},
		})
		pl := &stubPriorityLog{batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: tid, Value: makePayload("rt", "bt", []sop.Handle{mkHandle(lid, 1)})}}, removeErr: map[string]error{tid.String(): fmt.Errorf("rm err")}}
		tl := newTransactionLogger(stubTLog{pl: pl}, true)
		consumed, err := tl.doPriorityRollbacks(ctx, tx)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if !consumed {
			t.Fatalf("expected consumed=true due to batch present")
		}
	})

	t.Run("acquire_locks_conflict_returns_failover", func(t *testing.T) {
		redis := mocks.NewMockClient()
		reg := mocks.NewMockRegistry(false)
		tx := &Transaction{l2Cache: redis, registry: reg}
		tid := sop.NewUUID()
		lid := sop.NewUUID()
		// Seed registry so version check would pass if reached
		_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "rt", IDs: []sop.Handle{mkHandle(lid, 1)}},
		})
		// Pre-lock the key by someone else to force acquireLocks error with owner mismatch
		k := tx.l2Cache.CreateLockKeys([]string{lid.String()})[0].Key
		other := sop.NewUUID()
		_ = tx.l2Cache.Set(ctx, k, other.String(), time.Minute)
		pl := &stubPriorityLog{batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: tid, Value: makePayload("rt", "bt", []sop.Handle{mkHandle(lid, 1)})}}}
		tl := newTransactionLogger(stubTLog{pl: pl}, true)
		consumed, err := tl.doPriorityRollbacks(ctx, tx)
		if consumed {
			t.Fatalf("expected consumed=false when conflict")
		}
		if se, ok := err.(sop.Error); !ok || se.Code != sop.RestoreRegistryFileSectorFailure {
			t.Fatalf("expected failover error, got %v", err)
		}
	})

	t.Run("version_too_far_returns_failover", func(t *testing.T) {
		redis := mocks.NewMockClient()
		reg := mocks.NewMockRegistry(false)
		tx := &Transaction{l2Cache: redis, registry: reg}
		tid := sop.NewUUID()
		lid := sop.NewUUID()
		// Seed registry with version too far ahead (e.g., 5 vs uh 3) to trigger failover
		_ = reg.Add(ctx, []sop.RegistryPayload[sop.Handle]{
			{RegistryTable: "rt", IDs: []sop.Handle{mkHandle(lid, 5)}},
		})
		pl := &stubPriorityLog{batch: []sop.KeyValuePair[sop.UUID, []sop.RegistryPayload[sop.Handle]]{{Key: tid, Value: makePayload("rt", "bt", []sop.Handle{mkHandle(lid, 3)})}}}
		tl := newTransactionLogger(stubTLog{pl: pl}, true)
		consumed, err := tl.doPriorityRollbacks(ctx, tx)
		if consumed {
			t.Fatalf("expected consumed=false on version failover")
		}
		if se, ok := err.(sop.Error); !ok || se.Code != sop.RestoreRegistryFileSectorFailure {
			t.Fatalf("expected failover error, got %v", err)
		}
	})
}
