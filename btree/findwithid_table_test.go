package btree

import (
    "testing"

    "github.com/sharedcode/sop"
)

func TestFindWithID_TableMore(t *testing.T) {
    store := sop.NewStoreInfo(sop.StoreOptions{SlotLength: 4, IsUnique: false})
    fnr := &fakeNR[int, string]{n: map[sop.UUID]*Node[int, string]{}}
    si := StoreInterface[int, string]{NodeRepository: fnr, ItemActionTracker: fakeIAT[int, string]{}}
    b, _ := New[int, string](store, &si, nil)

    // Root node
    root := newNode[int, string](b.getSlotLength())
    root.newID(sop.NilUUID)
    b.StoreInfo.RootNodeID = root.ID
    fnr.Add(root)

    // Insert three duplicates with known IDs
    ids := []sop.UUID{sop.NewUUID(), sop.NewUUID(), sop.NewUUID()}
    for _, id := range ids {
        v := "v"; vv := v
        if ok, err := b.AddItem(nil, &Item[int, string]{Key: 5, Value: &vv, ID: id}); !ok || err != nil {
            t.Fatalf("seed dup add err=%v", err)
        }
    }

    tests := []struct{
        name string
        id   sop.UUID
        wantOK bool
        wantID sop.UUID
    }{
        {"first", ids[0], true, ids[0]},
        {"middle", ids[1], true, ids[1]},
        {"last", ids[2], true, ids[2]},
        {"notfound", sop.NewUUID(), false, sop.NilUUID},
    }
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            ok, err := b.FindWithID(nil, 5, tc.id)
            if err != nil { t.Fatalf("FindWithID err: %v", err) }
            if ok != tc.wantOK { t.Fatalf("ok=%v want %v", ok, tc.wantOK) }
            if ok {
                got, _ := b.GetCurrentItem(nil)
                if got.ID != tc.wantID { t.Fatalf("got ID %v want %v", got.ID, tc.wantID) }
            }
        })
    }
}
