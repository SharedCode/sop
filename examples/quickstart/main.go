// Quickstart: the smallest possible SOP program.
// Uses the in-memory B-Tree, so it runs with zero infrastructure:
//
//	go run ./examples/quickstart
package main

import (
	"fmt"

	"github.com/sharedcode/sop/inmemory"
)

func main() {
	fmt.Println("SOP quickstart: in-memory ordered B-Tree")

	// Unique keys, string values.
	b3 := inmemory.NewBtree[int, string](true)

	// Add a few build records keyed by build number.
	builds := map[int]string{
		101: "commit 9f2c1a  build ok",
		102: "commit 4be77d  build ok",
		103: "commit c30e52  tests failed",
		104: "commit 8d91f0  build ok",
		105: "commit 77aa3e  released",
	}
	for k, v := range builds {
		if !b3.Add(k, v) {
			fmt.Printf("Add(%d) failed\n", k)
			return
		}
	}
	fmt.Printf("added %d items, count=%d\n", len(builds), b3.Count())

	// Point lookup.
	if b3.Find(103, true) {
		fmt.Printf("Find(103): %s\n", b3.GetCurrentValue())
	}

	// Update in place.
	b3.Update(103, "commit c30e52  tests fixed, build ok")
	b3.Find(103, true)
	fmt.Printf("after Update(103): %s\n", b3.GetCurrentValue())

	// Ordered range scan, no sort call needed: the tree keeps keys sorted.
	fmt.Println("ordered scan:")
	for ok := b3.First(); ok; ok = b3.Next() {
		fmt.Printf("  build %d -> %s\n", b3.GetCurrentKey(), b3.GetCurrentValue())
	}

	fmt.Println("quickstart: OK")
}
