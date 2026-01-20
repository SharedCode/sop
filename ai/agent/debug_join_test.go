package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/jsondb"
)

func TestReproJoinResult(t *testing.T) {
	// 1. Setup Data
	// Users: key=user_id
	usersItems := []MockItem{
		{Key: "u1", Value: map[string]any{"first_name": "John", "last_name": "Doe", "user_id": "u1"}},
		{Key: "u2", Value: map[string]any{"first_name": "Jane", "last_name": "Doe", "user_id": "u2"}},
	}

	// Users_Orders: key=user_id, value=order_id (Primitive STRING)
	bridgeItems := []MockItem{
		{Key: "u1", Value: "o100"},
		{Key: "u2", Value: "o101"},
	}

	// Orders: key=order_id
	ordersItems := []MockItem{
		{Key: "o100", Value: map[string]any{"id": "o100", "total_amount": 600, "date": "2023-01-01"}},
		{Key: "o101", Value: map[string]any{"id": "o101", "total_amount": 100, "date": "2023-01-02"}},
	}

	usersStore := NewMockStore("users", usersItems)
	bridgeStore := NewMockStore("users_orders", bridgeItems)
	ordersStore := NewMockStore("orders", ordersItems)

	stores := map[string]jsondb.StoreAccessor{
		"users":        usersStore,
		"users_orders": bridgeStore,
		"orders":       ordersStore,
	}

	// 2. Setup Context
	sc := NewScriptContext()
	sc.Stores = stores

	engine := NewScriptEngine(sc, nil)

	// 3. Script Execution (Mimic User Script)

	// Step A: Scan Users Filter John
	argsScan := map[string]any{
		"store":  "users",
		"filter": map[string]any{"first_name": map[string]any{"$eq": "John"}},
	}
	scanRes, err := engine.Scan(context.Background(), argsScan)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Step B: Join users_orders
	// Args: alias=users_orders (implied by store name)
	// On: user_id = key
	argsJoin1 := map[string]any{
		"store": "users_orders",
		"on":    map[string]any{"user_id": "key"},
	}
	join1Res, err := engine.JoinRight(context.Background(), scanRes, argsJoin1)
	if err != nil {
		t.Fatalf("Join1 failed: %v", err)
	}

	// Debug Join 1 Result
	t.Logf("Join1 Result: %T", join1Res)
	// Do NOT consume cursor here, pass it to next step directly

	// Step C: Join orders
	// Args: alias=orders
	argsJoin2 := map[string]any{
		"store": "orders",
		"alias": "orders", // Explicit alias
		"on":    map[string]any{"users_orders.value": "id"},
	}
	join2Res, err := engine.JoinRight(context.Background(), join1Res, argsJoin2)
	if err != nil {
		t.Fatalf("Join2 failed: %v", err)
	}

	// Step D: Project
	// "project": { "select": ["first_name", "last_name", "orders"] }
	// "orders" is the alias for the last join.
	argsProj := map[string]any{
		"select": []any{"first_name", "last_name", "orders"},
	}
	projRes, err := engine.Project(context.Background(), join2Res, argsProj)
	if err != nil {
		t.Fatalf("Project failed: %v", err)
	}

	// Verify Result
	t.Logf("Final Result Wrapper: %T", projRes)

	var allItems []any
	if cursor, ok := projRes.(ScriptCursor); ok {
		for {
			item, ok, err := cursor.Next(context.Background())
			if err != nil {
				t.Fatalf("Cursor error: %v", err)
			}
			if !ok {
				break
			}
			allItems = append(allItems, item)
		}
	} else if list, ok := projRes.([]any); ok {
		allItems = list
	} else {
		t.Fatalf("Unknown result type: %T", projRes)
	}

	t.Logf("Final Items: %+v", allItems)

	if len(allItems) == 0 {
		t.Error("Final result is empty, expected 1 record (John -> o100)")
	}
}
