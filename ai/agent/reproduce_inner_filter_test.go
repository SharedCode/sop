package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/jsondb"
)

func TestReproInnerJoinFilter(t *testing.T) {
	// 1. Setup Data
	// Users: u1
	usersItems := []MockItem{
		{Key: "u1", Value: map[string]any{"first_name": "John", "user_id": "u1"}},
	}

	// Bridge: u1 -> o100
	bridgeItems := []MockItem{
		// Note: The script joins users_orders on user_id=key.
		// Bridge store in previous example had Key=u1, Value=o100.
		{Key: "u1", Value: "o100"},
	}

	// Orders: o100 -> {total_amount: 600}
	ordersItems := []MockItem{
		{Key: "o100", Value: map[string]any{"total_amount": 600}},
	}

	usersStore := NewMockStore("users", usersItems)
	bridgeStore := NewMockStore("users_orders", bridgeItems)
	ordersStore := NewMockStore("orders", ordersItems)

	stores := map[string]jsondb.StoreAccessor{
		"users":        usersStore,
		"users_orders": bridgeStore,
		"orders":       ordersStore,
	}

	sc := NewScriptContext()
	sc.Stores = stores
	engine := NewScriptEngine(sc, nil)

	// A. Scan Users
	scanRes, _ := engine.Scan(context.Background(), map[string]any{
		"store":  "users",
		"filter": map[string]any{"first_name": map[string]any{"$eq": "John"}},
	})

	// B. Join users_orders (Inner)
	// join_right in prompt calls Join logic.
	// script: op="join", type="inner", with="users_orders", on={user_id: key}
	join1Res, err := engine.Join(context.Background(), scanRes, map[string]any{
		"with": "users_orders",
		"type": "inner",
		"on":   map[string]any{"user_id": "key"},
	})
	if err != nil {
		t.Fatalf("Join1 failed: %v", err)
	}

	// C. Join orders (Inner)
	// script: op="join", type="inner", with="orders", on={value: id}
	// "value" (from bridge) == "id" (from orders)
	join2Res, err := engine.Join(context.Background(), join1Res, map[string]any{
		"with": "orders",
		"type": "inner",
		"on":   map[string]any{"value": "key"},
	})
	if err != nil {
		t.Fatalf("Join2 failed: %v", err)
	}

	// Inspect Join2 Result to see keys
	if cursor, ok := join2Res.(ScriptCursor); ok {
		item, ok, _ := cursor.Next(context.Background())
		if ok {
			t.Logf("Join2 Item: %+v", item)
			// Put it back? No, just listify or peek.
			// We just want to see if it failed here or later.
			// Since filter follows, consuming one item breaks filter count.
			// But for debugging let's fail if empty.
		} else {
			t.Fatalf("Join2 returned empty result before filter!")
		}
	}

	// RESET cursor? ScriptCursor might not support Reset.
	// Re-run the chain to feed Filter properly.

	// Re-run for Filter test
	scanRes, _ = engine.Scan(context.Background(), map[string]any{
		"store":  "users",
		"filter": map[string]any{"first_name": map[string]any{"$eq": "John"}},
	})
	join1Res, _ = engine.Join(context.Background(), scanRes, map[string]any{
		"with": "users_orders",
		"type": "inner",
		"on":   map[string]any{"user_id": "key"},
	})
	join2Res, _ = engine.Join(context.Background(), join1Res, map[string]any{
		"with": "orders",
		"type": "inner",
		"on":   map[string]any{"value": "key"},
	})

	// D. Filter total_amount > 500
	// The Join2 Result likely has "orders.total_amount".
	// The filter uses "total_amount".
	filterRes, err := engine.Filter(context.Background(), join2Res, map[string]any{
		"condition": map[string]any{
			"total_amount": map[string]any{"$gt": 500},
		},
	})
	if err != nil {
		t.Fatalf("Filter failed: %v", err)
	}

	// Check count
	// Use stageUpdate/Delete or just manual list conversion to check result
	resList, _ := engine.Update(context.Background(), filterRes, map[string]any{"store": "users"}) // Hack to listify

	t.Logf("Filter Output: %+v", resList)
	if len(resList) == 0 {
		t.Fatal("Filter returned empty! Expecting 1 record.")
	}
}
