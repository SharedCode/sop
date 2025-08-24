//go:build integration
// +build integration

package integrationtests

import (
    "cmp"
    "context"
    "testing"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/fs"
    "github.com/sharedcode/sop/inredfs"
)

func Test_Basic_EC_Short(t *testing.T) {
    ctx := context.Background()
    to, _ := inredfs.NewTransactionOptionsWithReplication(sop.ForWriting, -1, fs.MinimumModValue, storesFolders, nil)
    trans, err := inredfs.NewTransactionWithReplication(ctx, to)
    if err != nil { t.Fatal(err) }
    trans.Begin()
    b3, err := inredfs.NewBtreeWithReplication[int, string](ctx, sop.StoreOptions{
    // Use a unique name to avoid colliding with prior runs/configs
    Name: "barstoreec_short_it", SlotLength: 8, IsValueDataInNodeSegment: true,
    }, trans, cmp.Compare)
    if err != nil { t.Fatal(err) }

    if ok, err := b3.Add(ctx, 1, "hello world"); !ok || err != nil { t.Fatalf("add err: %v", err) }
    if ok, err := b3.Find(ctx, 1, false); !ok || err != nil { t.Fatalf("find err: %v", err) }
    if v, err := b3.GetCurrentValue(ctx); err != nil || v != "hello world" { t.Fatalf("got %v err %v", v, err) }

    if err := trans.Commit(ctx); err != nil { t.Fatal(err) }
}
