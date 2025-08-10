package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
)

func Test_ManageBtree_OpenNewBtree_Cases(t *testing.T) {
	ctx := context.Background()
	cmp := func(a, b int) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}

	type preFn func(t *testing.T, ctx context.Context, trans sop.Transaction)
	cases := []struct {
		name        string
		op          string // "open", "new", "new-dup"
		begin       bool
		storeName   string
		so          sop.StoreOptions
		pre         preFn
		expectErr   bool
		expectEnded bool
	}{
		{name: "open_nil_transaction_error", op: "open", begin: false, storeName: "", pre: nil, expectErr: true},
		{name: "open_not_begun_error", op: "open", begin: false, storeName: "s1", pre: nil, expectErr: true},
		{name: "open_empty_name_error", op: "open", begin: true, storeName: "", pre: nil, expectErr: true},
		{name: "open_nonexistent_store_rolls_back", op: "open", begin: true, storeName: "does_not_exist", pre: nil, expectErr: true, expectEnded: true},

		{name: "new_not_begun_error", op: "new", begin: false, so: sop.StoreOptions{Name: "x"}, pre: nil, expectErr: true},
		{name: "new_empty_name_error", op: "new", begin: true, so: sop.StoreOptions{Name: ""}, pre: nil, expectErr: true},
		{name: "new_with_ttl_path_success", op: "new", begin: true, so: sop.StoreOptions{Name: "ttl_store", SlotLength: 2, CacheConfig: &sop.StoreCacheConfig{StoreInfoCacheDuration: time.Minute, IsStoreInfoCacheTTL: true}}},
		{name: "new_incompatible_config_rolls_back", op: "new", begin: true, so: sop.StoreOptions{Name: "store_incompat", SlotLength: 6}, pre: func(t *testing.T, ctx context.Context, trans sop.Transaction) {
			t.Helper()
			t2 := trans.GetPhasedTransaction().(*Transaction)
			_ = t2.StoreRepository.Add(ctx, sop.StoreInfo{Name: "store_incompat", SlotLength: 4})
		}, expectErr: true, expectEnded: true},
		{name: "new_duplicate_in_transaction_rolls_back", op: "new-dup", begin: true, so: sop.StoreOptions{Name: "dup_store", SlotLength: 2}, expectErr: true, expectEnded: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Special-case: nil transaction for the first open case
			if tc.op == "open" && !tc.begin && tc.storeName == "" && tc.expectErr {
				if _, err := OpenBtree[int, int](ctx, "", nil, nil); err == nil {
					t.Fatalf("expected error for nil transaction")
				}
				return
			}

			trans, _ := newMockTransaction(t, sop.ForWriting, -1)
			if tc.begin {
				_ = trans.Begin()
			} else {
				if trans.HasBegun() {
					_ = trans.Close()
				}
			}
			if tc.pre != nil {
				tc.pre(t, ctx, trans)
			}

			switch tc.op {
			case "open":
				_, err := OpenBtree[int, int](ctx, tc.storeName, trans, cmp)
				if tc.expectErr {
					if err == nil {
						t.Fatalf("expected error, got nil")
					}
				} else if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			case "new":
				_, err := NewBtree[int, int](ctx, tc.so, trans, cmp)
				if tc.expectErr {
					if err == nil {
						t.Fatalf("expected error, got nil")
					}
				} else if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			case "new-dup":
				if _, err := NewBtree[int, int](ctx, tc.so, trans, cmp); err != nil {
					t.Fatalf("unexpected first NewBtree error: %v", err)
				}
				if _, err := NewBtree[int, int](ctx, tc.so, trans, cmp); err == nil {
					t.Fatalf("expected duplicate error on second NewBtree")
				}
			}

			if tc.expectEnded && trans.HasBegun() {
				t.Fatalf("expected transaction to be ended after error/rollback")
			}
			_ = trans.Close()
		})
	}
}

// finalizeCommit has payload but lastCommittedFunctionLog < deleteObsoleteEntries: it should continue without deletions.
// finalizeCommit payload-continue covered in transactionlogger_unit_test table-driven cases.
