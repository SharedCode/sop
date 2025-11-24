package functions

import (
	"github.com/sharedcode/sop/ai/internal/adapter/storage"
	"github.com/sharedcode/sop/ai/internal/policy"
	"testing"
)

func TestIntegrityFoundAndMissing(t *testing.T) {
	s, err := storage.Open("flat", map[string]any{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := s.Begin(false)
	if err := tx.Put([]byte("alpha"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fn := NewIntegrity(policy.NewAllow("pol"), s)
	outFound, _ := fn.Invoke(map[string]any{"key": "alpha"})
	if outFound["valid"] != true {
		t.Fatalf("expected valid true")
	}
	if outFound["checksum"] == "" {
		t.Fatalf("expected checksum")
	}
	outMissing, _ := fn.Invoke(map[string]any{"key": "missing"})
	if outMissing["valid"].(bool) != false {
		t.Fatalf("expected invalid for missing")
	}
}
