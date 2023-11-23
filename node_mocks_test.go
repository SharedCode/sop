package sop

import (
	"fmt"
	"testing"

	sop "github.com/SharedCode/sop/in_memory"
)

func Test_MockDistributeItemOnNodeWithNilChild(t *testing.T) {
	fmt.Printf("Btree hello world.\n")
	b3 := sop.NewBtree[int, string](false)

	b3.Add(5000, "I am the value with 5000 key.")
	b3.Add(5001, "I am the value with 5001 key.")
	b3.Add(5000, "I am also a value with 5000 key.")

	if !b3.FindOne(5000, true) || b3.GetCurrentKey() != 5000 {
		t.Errorf("FindOne(5000, true) failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if !b3.MoveToNext() || b3.GetCurrentKey() != 5000 {
		t.Errorf("MoveToNext() failed, got = %v, want = 5000", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())

	if !b3.MoveToNext() || b3.GetCurrentKey() != 5001 {
		t.Errorf("MoveToNext() failed, got = %v, want = 5001", b3.GetCurrentKey())
	}
	fmt.Printf("Hello, %s.\n", b3.GetCurrentValue())
	fmt.Printf("Btree hello world ended.\n\n")
}
