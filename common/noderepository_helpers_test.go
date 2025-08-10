package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
)

func Test_ConvertHelpers_MapNodesToPayloads(t *testing.T) {
	ctx := context.Background()
	_ = ctx
	// Build store and two nodes
	so := sop.StoreOptions{Name: "nr_helpers", SlotLength: 8, IsValueDataInNodeSegment: true}
	si := sop.NewStoreInfo(so)
	n1 := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 1}
	n2 := &btree.Node[PersonKey, Person]{ID: sop.NewUUID(), Version: 2}
	nodes := []sop.Tuple[*sop.StoreInfo, []interface{}]{
		{First: si, Second: []interface{}{n1, n2}},
	}
	bibs := convertToBlobRequestPayload(nodes)
	if len(bibs) != 1 || len(bibs[0].Blobs) != 2 {
		t.Fatalf("unexpected blob payload shape: %+v", bibs)
	}
	if bibs[0].Blobs[0] != n1.ID || bibs[0].Blobs[1] != n2.ID {
		t.Fatalf("blob IDs mismatch: got %v want [%s %s]", bibs[0].Blobs, n1.ID.String(), n2.ID.String())
	}
	vids := convertToRegistryRequestPayload(nodes)
	if len(vids) != 1 || len(vids[0].IDs) != 2 {
		t.Fatalf("unexpected registry payload shape: %+v", vids)
	}
	if vids[0].IDs[0] != n1.ID || vids[0].IDs[1] != n2.ID {
		t.Fatalf("registry IDs mismatch: got %v want [%s %s]", vids[0].IDs, n1.ID.String(), n2.ID.String())
	}
}

func Test_ExtractInactiveBlobsIDs(t *testing.T) {
	// Build handles where one of them has an inactive ID populated
	l1 := sop.NewUUID()
	l2 := sop.NewUUID()
	h1 := sop.NewHandle(l1)
	h1.PhysicalIDB = sop.NewUUID()
	h1.IsActiveIDB = true // inactive is A
	h2 := sop.NewHandle(l2)
	// h2 inactive remains zero
	payload := []sop.RegistryPayload[sop.Handle]{
		{BlobTable: "tbl", IDs: []sop.Handle{h1, h2}},
	}
	b := extractInactiveBlobsIDs(payload)
	if len(b) != 1 || len(b[0].Blobs) != 1 || b[0].Blobs[0] != h1.PhysicalIDA {
		t.Fatalf("unexpected inactive blobs: %+v", b)
	}
}

func Test_NodeRepository_ActivateAndTouch(t *testing.T) {
	// Prepare handles
	h1 := sop.NewHandle(sop.NewUUID())
	h1.PhysicalIDB = sop.NewUUID()
	h1.IsActiveIDB = false // active A, inactive B
	h1.Version = 5
	h1.WorkInProgressTimestamp = 0

	h2 := sop.NewHandle(sop.NewUUID())
	h2.PhysicalIDB = sop.NewUUID()
	h2.IsActiveIDB = true // active B, inactive A
	h2.Version = 2
	h2.WorkInProgressTimestamp = 7

	set := []sop.RegistryPayload[sop.Handle]{{IDs: []sop.Handle{h1, h2}}}
	nr := &nodeRepositoryBackend{}

	// Activate should flip active and set WIP timestamp to 1 and bump version
	out, err := nr.activateInactiveNodes(set)
	if err != nil {
		t.Fatalf("activateInactiveNodes error: %v", err)
	}
	if !out[0].IDs[0].IsActiveIDB || out[0].IDs[0].Version != 6 || out[0].IDs[0].WorkInProgressTimestamp != 1 {
		t.Fatalf("unexpected h1 after activate: %+v", out[0].IDs[0])
	}
	if out[0].IDs[1].IsActiveIDB || out[0].IDs[1].Version != 3 || out[0].IDs[1].WorkInProgressTimestamp != 1 {
		t.Fatalf("unexpected h2 after activate: %+v", out[0].IDs[1])
	}

	// Touch should bump version and clear WIP
	out2, err := nr.touchNodes(out)
	if err != nil {
		t.Fatalf("touchNodes error: %v", err)
	}
	if out2[0].IDs[0].Version != 7 || out2[0].IDs[0].WorkInProgressTimestamp != 0 {
		t.Fatalf("unexpected h1 after touch: %+v", out2[0].IDs[0])
	}
	if out2[0].IDs[1].Version != 4 || out2[0].IDs[1].WorkInProgressTimestamp != 0 {
		t.Fatalf("unexpected h2 after touch: %+v", out2[0].IDs[1])
	}
}
