package storage

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/internal/port"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
)

type sopStore struct {
	name        string
	activePath  string
	passivePath string
	trans       sop.Transaction
	btree       btree.BtreeInterface[string, []byte]
	ctx         context.Context
}

type sopTx struct {
	store    *sopStore
	readOnly bool
}

func init() {
	Register("sop", func(cfg map[string]any) (port.KVStore, error) {
		activePath, _ := cfg["active_path"].(string)
		if activePath == "" {
			activePath = "./data/sop_active"
		}
		passivePath, _ := cfg["passive_path"].(string)
		if passivePath == "" {
			passivePath = "./data/sop_passive"
		}

		return &sopStore{
			name:        "ai_store",
			activePath:  activePath,
			passivePath: passivePath,
			ctx:         context.Background(),
		}, nil
	})
}

func (s *sopStore) Begin(readOnly bool) (port.Tx, error) {
	// Initialize SOP transaction
	// Note: This requires Redis and local FS setup as per SOP requirements.
	// We assume default config for simplicity here.

	storesFolders := []string{s.activePath, s.passivePath}

	mode := sop.ForWriting
	if readOnly {
		mode = sop.ForReading
	}

	// Define Erasure Config
	erasureConfig := map[string]fs.ErasureCodingConfig{
		"": {
			DataShardsCount:             1,
			ParityShardsCount:           1,
			BaseFolderPathsAcrossDrives: storesFolders,
			RepairCorruptedShards:       false,
		},
	}

	to, err := inredfs.NewTransactionOptionsWithReplication(mode, -1, -1, storesFolders, erasureConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction options: %w", err)
	}

	trans, err := inredfs.NewTransactionWithReplication(s.ctx, to)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}

	if err := trans.Begin(s.ctx); err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Open or Create B-Tree
	var b3 btree.BtreeInterface[string, []byte]
	if readOnly {
		b3, err = inredfs.OpenBtreeWithReplication[string, []byte](s.ctx, s.name, trans, nil)
	} else {
		b3, err = inredfs.NewBtreeWithReplication[string, []byte](s.ctx, sop.StoreOptions{
			Name:       s.name,
			SlotLength: 100,
		}, trans, nil)
	}

	if err != nil {
		trans.Rollback(s.ctx)
		return nil, fmt.Errorf("failed to open/create btree: %w", err)
	}

	s.trans = trans
	s.btree = b3

	return &sopTx{store: s, readOnly: readOnly}, nil
}

func (s *sopStore) Capabilities() port.Capabilities {
	return port.Capabilities{SupportsBatch: true, DurableWrites: true, MaxValueSize: 1 << 20}
}

func (s *sopStore) Stats() (map[string]any, error) {
	return map[string]any{"backend": "sop"}, nil
}

func (s *sopStore) Close() error {
	return nil
}

func (tx *sopTx) Get(key []byte) ([]byte, error) {
	found, err := tx.store.btree.Find(tx.store.ctx, string(key), false)
	if err != nil {
		return nil, fmt.Errorf("btree find failed: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("key not found")
	}
	val, err := tx.store.btree.GetCurrentValue(tx.store.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current value: %w", err)
	}
	return val, nil
}

func (tx *sopTx) Put(key []byte, val []byte) error {
	if tx.readOnly {
		return fmt.Errorf("transaction is read-only")
	}
	_, err := tx.store.btree.Add(tx.store.ctx, string(key), val)
	if err != nil {
		return fmt.Errorf("btree add failed: %w", err)
	}
	return nil
}

func (tx *sopTx) Delete(key []byte) error {
	if tx.readOnly {
		return fmt.Errorf("transaction is read-only")
	}
	_, err := tx.store.btree.Remove(tx.store.ctx, string(key))
	if err != nil {
		return fmt.Errorf("btree remove failed: %w", err)
	}
	return nil
}

func (tx *sopTx) Commit() error {
	if err := tx.store.trans.Commit(tx.store.ctx); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}
	return nil
}

func (tx *sopTx) Rollback() error {
	if err := tx.store.trans.Rollback(tx.store.ctx); err != nil {
		return fmt.Errorf("transaction rollback failed: %w", err)
	}
	return nil
}
