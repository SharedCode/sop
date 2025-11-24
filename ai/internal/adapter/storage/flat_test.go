package storage

import "testing"

func TestFlatBasic(t *testing.T) {
	s, err := Open("flat", map[string]any{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("alpha")
	val := []byte("value-1")
	if err := tx.Put(key, val); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx2, err := s.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := tx2.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(val) {
		t.Fatalf("mismatch %q != %q", got, val)
	}
}
