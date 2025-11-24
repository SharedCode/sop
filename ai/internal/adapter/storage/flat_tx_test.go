package storage

import "testing"

func TestFlatRollbackAndDelete(t *testing.T) {
	s, err := Open("flat", map[string]any{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := s.Begin(false)
	if err := tx.Put([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx2, _ := s.Begin(true)
	if _, err := tx2.Get([]byte("a")); err == nil {
		t.Fatalf("expected not found after rollback")
	}
	_ = tx2.Rollback()
	// commit then delete
	tx3, _ := s.Begin(false)
	_ = tx3.Put([]byte("b"), []byte("two"))
	_ = tx3.Commit()
	tx4, _ := s.Begin(false)
	_ = tx4.Delete([]byte("b"))
	_ = tx4.Commit()
	tx5, _ := s.Begin(true)
	if _, err := tx5.Get([]byte("b")); err == nil {
		t.Fatalf("expected not found after delete")
	}
	_ = tx5.Rollback()
}
