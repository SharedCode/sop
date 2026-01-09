package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
)

func TestToolJoin_ProjectionOrder_WithFieldsString(t *testing.T) {
	ctx := context.Background()
	dbPath := "test_join_projection_order"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{StoresFolders: []string{dbPath}, CacheType: sop.InMemory}
	sysDB := database.NewDatabase(dbOpts)

	adminAgent := &DataAdminAgent{
		Config:    Config{ID: "sql_admin"},
		databases: map[string]sop.DatabaseOptions{"default": dbOpts},
		systemDB:  sysDB,
	}

	tx, err := sopdb.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	deptStore, err := sopdb.NewBtree[string, any](ctx, dbOpts, "department", tx, nil)
	if err != nil {
		t.Fatalf("NewBtree department failed: %v", err)
	}
	empStore, err := sopdb.NewBtree[string, any](ctx, dbOpts, "employee", tx, nil)
	if err != nil {
		t.Fatalf("NewBtree employee failed: %v", err)
	}
	deptStore.Add(ctx, "d1", map[string]any{"region": "APAC", "department": "HR"})
	empStore.Add(ctx, "e1", map[string]any{"region": "APAC", "department": "HR", "name": "Employee 14"})
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "default"})

	args := map[string]any{
		"database":          "default",
		"left_store":        "department",
		"right_store":       "employee",
		"join_type":         "inner",
		"left_join_fields":  []string{"region", "department"},
		"right_join_fields": []string{"region", "department"},
		"fields":            "a.region, a.department, b.name as employee",
		"limit":             4,
	}

	res, err := adminAgent.toolJoin(ctx, args)
	if err != nil {
		t.Fatalf("toolJoin failed: %v", err)
	}

	// Verify order in raw JSON output (map-based unmarshalling would lose ordering).
	regionIdx := strings.Index(res, `"Region"`)
	deptIdx := strings.Index(res, `"Department"`)
	empIdx := strings.Index(res, `"employee"`)
	if regionIdx == -1 || deptIdx == -1 || empIdx == -1 {
		t.Fatalf("Missing expected fields in output: %s", res)
	}
	if !(regionIdx < deptIdx && deptIdx < empIdx) {
		t.Fatalf("Projection order not preserved. Expected Region then Department then employee. Got: %s", res)
	}
}
