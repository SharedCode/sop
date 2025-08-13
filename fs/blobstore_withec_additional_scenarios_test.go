package fs

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/sharedcode/sop"
)

// stubECFileIO lets us induce per-shard write failures.
type stubECFileIO struct {
	failAll    bool
	failFirst  bool
	mu         sync.Mutex // guards writeCalls for race-free -race execution
	writeCalls int
}

func (s *stubECFileIO) WriteFile(ctx context.Context, name string, data []byte, perm os.FileMode) error {
	s.mu.Lock()
	s.writeCalls++
	callNum := s.writeCalls
	failAll := s.failAll
	failFirst := s.failFirst
	s.mu.Unlock()
	if failAll {
		return errors.New("induced write error all")
	}
	if failFirst && callNum == 1 {
		return errors.New("induced write error first")
	}
	return nil
}
func (s *stubECFileIO) ReadFile(ctx context.Context, name string) ([]byte, error) {
	return nil, errors.New("read not implemented")
}
func (s *stubECFileIO) Remove(context.Context, string) error                   { return nil }
func (s *stubECFileIO) Exists(context.Context, string) bool                    { return false }
func (s *stubECFileIO) RemoveAll(context.Context, string) error                { return nil }
func (s *stubECFileIO) MkdirAll(context.Context, string, os.FileMode) error    { return nil }
func (s *stubECFileIO) ReadDir(context.Context, string) ([]os.DirEntry, error) { return nil, nil }

// readErrFileIO triggers read errors for all shards to reach GetOne empty shards path.
type readErrFileIO struct{}

func (r readErrFileIO) WriteFile(context.Context, string, []byte, os.FileMode) error {
	return errors.New("no writes")
}
func (r readErrFileIO) ReadFile(context.Context, string) ([]byte, error) {
	return nil, errors.New("induced read fail")
}
func (r readErrFileIO) Remove(context.Context, string) error                   { return nil }
func (r readErrFileIO) Exists(context.Context, string) bool                    { return false }
func (r readErrFileIO) RemoveAll(context.Context, string) error                { return nil }
func (r readErrFileIO) MkdirAll(context.Context, string, os.FileMode) error    { return nil }
func (r readErrFileIO) ReadDir(context.Context, string) ([]os.DirEntry, error) { return nil, nil }

// Helper to build minimal EC config.
func testECConfig(table string, data, parity int) map[string]ErasureCodingConfig {
	total := data + parity
	base := make([]string, total)
	for i := 0; i < total; i++ {
		base[i] = "d" + string(rune('0'+i))
	}
	return map[string]ErasureCodingConfig{
		table: {DataShardsCount: data, ParityShardsCount: parity, BaseFolderPathsAcrossDrives: base, RepairCorruptedShards: false},
	}
}

func TestBlobStoreWithEC_Add_FailureToleranceAndExceed(t *testing.T) {
	ctx := context.Background()
	table := "tbl"
	cfg := testECConfig(table, 1, 1)

	// Tolerated single failure
	fio1 := &stubECFileIO{failFirst: true}
	bs1, err := NewBlobStoreWithEC(DefaultToFilePath, fio1, cfg)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	payload := []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: table, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: sop.NewUUID(), Value: []byte("abc")}}}}
	if err := bs1.Add(ctx, payload); err != nil {
		t.Fatalf("expected tolerated add, got %v", err)
	}
	if fio1.writeCalls != 2 {
		t.Fatalf("expected 2 shard writes got %d", fio1.writeCalls)
	}

	// Exceed parity failures
	fio2 := &stubECFileIO{failAll: true}
	bs2, _ := NewBlobStoreWithEC(DefaultToFilePath, fio2, cfg)
	if err := bs2.Add(ctx, payload); err == nil {
		t.Fatalf("expected error exceeding parity tolerance")
	}
}

func TestBlobStoreWithEC_GetOne_AllReadsFail(t *testing.T) {
	ctx := context.Background()
	table := "tbl2"
	cfg := testECConfig(table, 1, 1)
	bs, err := NewBlobStoreWithEC(DefaultToFilePath, readErrFileIO{}, cfg)
	if err != nil {
		t.Fatalf("NewBlobStoreWithEC: %v", err)
	}
	if _, err := bs.GetOne(ctx, table, sop.NewUUID()); err == nil {
		t.Fatalf("expected read failure error")
	}
}
