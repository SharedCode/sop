package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/sharedcode/sop/ai/internal/port"
)

type flatStore struct {
	root string
	mu   sync.RWMutex
}

type flatTx struct {
	store    *flatStore
	readOnly bool
	writes   map[string][]byte
	deletes  map[string]struct{}
	closed   bool
}

func init() {
	Register("flat", func(cfg map[string]any) (port.KVStore, error) {
		root, _ := cfg["root"].(string)
		if root == "" {
			root = "./data/registry"
		}
		if err := os.MkdirAll(root, 0o755); err != nil {
			return nil, err
		}
		return &flatStore{root: root}, nil
	})
}

func (s *flatStore) Begin(readOnly bool) (port.Tx, error) {
	return &flatTx{store: s, readOnly: readOnly, writes: map[string][]byte{}, deletes: map[string]struct{}{}}, nil
}
func (s *flatStore) Close() error { return nil }
func (s *flatStore) Stats() (map[string]any, error) {
	entries, err := os.ReadDir(s.root)
	count := 0
	if err == nil {
		count = len(entries)
	}
	return map[string]any{"files": count, "root": s.root}, err
}
func (s *flatStore) Capabilities() port.Capabilities {
	return port.Capabilities{SupportsBatch: true, DurableWrites: true, MaxValueSize: 1 << 24}
}

func keyPath(root string, key []byte) string {
	h := sha256.Sum256(key)
	hexStr := hex.EncodeToString(h[:])
	return filepath.Join(root, hexStr[0:2], hexStr[2:4], hexStr)
}
func ensureDirs(path string) error { return os.MkdirAll(filepath.Dir(path), 0o755) }

func (tx *flatTx) Get(key []byte) ([]byte, error) {
	if tx.closed {
		return nil, errors.New("tx closed")
	}
	kp := keyPath(tx.store.root, key)
	if v, ok := tx.writes[kp]; ok {
		return v, nil
	}
	if _, del := tx.deletes[kp]; del {
		return nil, fmt.Errorf("not found")
	}
	f, err := os.Open(kp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("not found")
		}
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	return data, err
}
func (tx *flatTx) Put(key []byte, val []byte) error {
	if tx.closed {
		return errors.New("tx closed")
	}
	if tx.readOnly {
		return errors.New("read-only tx")
	}
	kp := keyPath(tx.store.root, key)
	tx.writes[kp] = append([]byte(nil), val...)
	delete(tx.deletes, kp)
	return nil
}
func (tx *flatTx) Delete(key []byte) error {
	if tx.closed {
		return errors.New("tx closed")
	}
	if tx.readOnly {
		return errors.New("read-only tx")
	}
	kp := keyPath(tx.store.root, key)
	tx.deletes[kp] = struct{}{}
	delete(tx.writes, kp)
	return nil
}
func (tx *flatTx) Commit() error {
	if tx.closed {
		return errors.New("tx closed")
	}
	if tx.readOnly {
		tx.closed = true
		return nil
	}
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	for path, data := range tx.writes {
		if err := ensureDirs(path); err != nil {
			return err
		}
		tmp := path + ".tmp"
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if _, err = f.Write(data); err != nil {
			f.Close()
			return err
		}
		if err = f.Sync(); err != nil {
			f.Close()
			return err
		}
		if err = f.Close(); err != nil {
			return err
		}
		if err = os.Rename(tmp, path); err != nil {
			return err
		}
	}
	for path := range tx.deletes {
		_ = os.Remove(path)
	}
	tx.closed = true
	return nil
}
func (tx *flatTx) Rollback() error { tx.closed = true; tx.writes = nil; tx.deletes = nil; return nil }
